package com.cs324.backend.worker;

import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.grpc.GrpcChannels;
import com.cs324.proto.BootstrapServiceGrpc;
import com.cs324.proto.Empty;
import io.grpc.Deadline;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Launches {@code WorkerClusterConfig.WORKER_COUNT} worker nodes (default 6,
 * minimum 6) as independent JVM processes. Each process gets its own gRPC
 * port, its own log file and its own pid file, so the workers stay fully
 * isolated while still talking to each other (and to the Bootstrap Node) over
 * gRPC.
 *
 * Usage:
 *   WorkerClusterLauncher [start|stop|status] [bootstrapHost] [bootstrapPort] [workerCount]
 */
public final class WorkerClusterLauncher {

    private static final long LOOKUP_TIMEOUT_MS = 20_000;
    private static final long LOOKUP_POLL_MS = 500;

    private WorkerClusterLauncher() {
    }

    public static void main(String[] args) throws Exception {
        String command = args.length > 0 ? args[0] : "start";
        // Apply an optional worker count BEFORE WorkerClusterConfig is first
        // loaded, so this JVM and every spawned worker agree on the cluster size.
        if (args.length > 3 && "start".equals(command)) {
            System.setProperty(WorkerClusterConfig.WORKER_COUNT_PROPERTY, args[3]);
        }
        String bootstrapHost = args.length > 1 ? args[1] : WorkerClusterConfig.DEFAULT_HOST;
        int bootstrapPort = args.length > 2 ? Integer.parseInt(args[2]) : BootstrapServer.DEFAULT_PORT;

        switch (command) {
            case "start" -> start(bootstrapHost, bootstrapPort);
            case "stop" -> stop();
            case "status" -> status();
            default -> {
                System.err.println("Unknown command: " + command);
                System.err.println("Usage: WorkerClusterLauncher [start|stop|status] [bootstrapHost] [bootstrapPort] [workerCount]");
                System.exit(1);
            }
        }
    }

    public static List<Integer> start(String bootstrapHost, int bootstrapPort) throws Exception {
        warnOnSizeMismatch(bootstrapHost, bootstrapPort);
        Path logDir = Paths.get(WorkerClusterConfig.LOG_DIR);
        Files.createDirectories(logDir);

        String classpath = currentClasspath();
        String javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        Path workingDir = Paths.get("").toAbsolutePath();

        List<Integer> started = new ArrayList<>();
        List<Integer> alreadyRunning = new ArrayList<>();

        for (int workerId : WorkerClusterConfig.workerIds()) {
            Path pidFile = logDir.resolve(pidFileName(workerId));
            Integer existingPid = readPid(pidFile);
            if (existingPid != null && isAlive(existingPid)) {
                alreadyRunning.add(workerId);
                System.out.println("Worker " + workerId + " already running with PID " + existingPid);
                continue;
            }

            int port = WorkerClusterConfig.portFor(workerId);
            if (isReachableOverGrpc(workerId, bootstrapHost)) {
                // A worker is already serving this port, but the pid file is stale or was
                // never written (e.g. launched manually, or left over from a previous
                // session). Do not spawn a duplicate; reclaim its tracking
                // so Start/Stop stay consistent.
                alreadyRunning.add(workerId);
                List<Long> orphans = findRunningPids(workerId, port);
                if (!orphans.isEmpty()) {
                    Files.writeString(pidFile, String.valueOf(orphans.get(0)), StandardCharsets.UTF_8);
                    System.out.println("Worker " + workerId + " already serving port " + port
                            + " (pid " + orphans.get(0) + ") - not started twice");
                } else {
                    deleteQuietly(pidFile);
                    System.out.println("Worker " + workerId + " already serving port " + port
                            + " (unmanaged process) - not started twice");
                }
                continue;
            }

            ProcessBuilder builder = new ProcessBuilder(
                    javaBin,
                    "-D" + WorkerClusterConfig.WORKER_COUNT_PROPERTY + "=" + WorkerClusterConfig.WORKER_COUNT,
                    "-cp", classpath,
                    WorkerServer.class.getName(),
                    String.valueOf(workerId),
                    String.valueOf(port),
                    bootstrapHost,
                    String.valueOf(bootstrapPort),
                    WorkerClusterConfig.DEFAULT_HOST);
            builder.directory(workingDir.toFile());
            builder.redirectOutput(ProcessBuilder.Redirect.to(logDir.resolve(logFileName(workerId)).toFile()));
            builder.redirectError(ProcessBuilder.Redirect.appendTo(logDir.resolve(logFileName(workerId)).toFile()));

            Process process = builder.start();
            Files.writeString(pidFile, String.valueOf(process.pid()), StandardCharsets.UTF_8);
            started.add(workerId);
            System.out.println("Started Worker " + workerId + " (pid " + process.pid()
                    + ", gRPC port " + port + ", log " + logDir.resolve(logFileName(workerId)) + ")");
        }

        if (!started.isEmpty()) {
            verifyOverGrpc(started, WorkerClusterConfig.DEFAULT_HOST);
        }

        System.out.println();
        System.out.println("Cluster state: " + (started.size() + alreadyRunning.size())
                + "/" + WorkerClusterConfig.WORKER_COUNT + " workers running"
                + (alreadyRunning.isEmpty() ? "" : " (already running: " + alreadyRunning + ")"));
        return started;
    }

    /** Warns when the bootstrap advertises a different cluster size than this launcher JVM. */
    private static void warnOnSizeMismatch(String bootstrapHost, int bootstrapPort) {
        try {
            BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap =
                    GrpcChannels.bootstrap(bootstrapHost, bootstrapPort);
            int expected = bootstrap.getClusterSize(Empty.getDefaultInstance()).getSize();
            if (expected != WorkerClusterConfig.WORKER_COUNT) {
                System.err.println("WARNING: Bootstrap Node is configured for " + expected
                        + " worker node(s) but this launcher is configured for "
                        + WorkerClusterConfig.WORKER_COUNT
                        + ". Launch via 'BootstrapServer [port] [workerCount]' or pass a matching "
                        + "workerCount so the whole cluster agrees.");
            }
        } catch (Exception e) {
            // Bootstrap not reachable yet - let the normal startup report that.
            System.err.println("Bootstrap Node not reachable at " + bootstrapHost + ":" + bootstrapPort
                    + " - start it first with 'java -cp target/classes:$(cat target/classpath.txt) "
                    + "com.cs324.backend.bootstrap.BootstrapServer [port] [workerCount]");
        }
    }

    /**
     * Polls each worker's own gRPC port until the worker answers GetWorkerState
     * with its own id.
     */
    private static void verifyOverGrpc(List<Integer> workerIds, String host) {
        long deadline = System.currentTimeMillis() + LOOKUP_TIMEOUT_MS;
        List<Integer> pending = new ArrayList<>(workerIds);

        while (!pending.isEmpty() && System.currentTimeMillis() < deadline) {
            pending.removeIf(workerId -> workerAnswers(workerId, host));
            if (!pending.isEmpty()) {
                try {
                    Thread.sleep(LOOKUP_POLL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        for (Integer workerId : workerIds) {
            if (pending.contains(workerId)) {
                System.err.println("Worker " + workerId + " did not answer its gRPC lookup within "
                        + LOOKUP_TIMEOUT_MS + " ms - check its log file");
            } else {
                System.out.println("Verified over gRPC: worker " + workerId
                        + " at " + host + ":" + WorkerClusterConfig.portFor(workerId));
            }
        }
    }

    public static void stop() {
        List<Integer> all = new ArrayList<>();
        for (int workerId : WorkerClusterConfig.workerIds()) {
            all.add(workerId);
        }
        stop(all);
    }

    /** Stops the given workers, taking care of orphaned (untracked) copies as well. */
    public static void stop(List<Integer> workerIds) {
        int stopped = 0;
        for (Integer workerId : workerIds) {
            Path pidFile = Paths.get(WorkerClusterConfig.LOG_DIR, pidFileName(workerId));
            Integer trackedPid = readPid(pidFile);
            int stoppedHere = 0;
            Set<Long> killed = new HashSet<>();

            if (trackedPid != null && isAlive(trackedPid)) {
                killed.add(trackedPid.longValue());
                if (stopPid(trackedPid.longValue())) {
                    stoppedHere++;
                }
            }
            for (long runningPid : findRunningPids(workerId, WorkerClusterConfig.portFor(workerId))) {
                if (killed.add(runningPid) && stopPid(runningPid)) {
                    stoppedHere++;
                }
            }

            deleteQuietly(pidFile);
            if (stoppedHere > 0) {
                System.out.println("Stopped Worker " + workerId + " (" + stoppedHere
                        + " process" + (stoppedHere == 1 ? "" : "es") + ")");
            } else {
                System.out.println("Worker " + workerId + " is not running");
            }
            stopped += stoppedHere;
        }
        System.out.println("Stopped " + stopped + " worker process(es)");
    }

    private static boolean stopPid(long pid) {
        return ProcessHandle.of(pid).map(handle -> {
            handle.destroy();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (handle.isAlive()) {
                handle.destroyForcibly();
            }
            return true;
        }).orElse(false);
    }

    /**
     * True when this worker's pid file exists and the recorded process is still
     * alive - i.e. the worker was launched (and is being tracked) by this
     * launcher.
     */
    public static boolean isManagedAndAlive(int workerId) {
        Path pidFile = Paths.get(WorkerClusterConfig.LOG_DIR, pidFileName(workerId));
        Integer pid = readPid(pidFile);
        return pid != null && isAlive(pid);
    }

    /** Lightweight check that a worker currently answers its gRPC server with its own id. */
    private static boolean isReachableOverGrpc(int workerId, String host) {
        return workerAnswers(workerId, host);
    }

    private static boolean workerAnswers(int workerId, String host) {
        try {
            int got = GrpcChannels.cluster(host, WorkerClusterConfig.portFor(workerId))
                    .withDeadline(Deadline.after(WorkerClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS,
                            TimeUnit.MILLISECONDS))
                    .getWorkerState(Empty.getDefaultInstance()).getWorkerId();
            return got == workerId;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Finds every live JVM running our WorkerServer command line for <i>workerId</i>
     * on <i>port</i>, regardless of whether a pid file tracks it. This is what lets
     * Start avoid duplicates and Stop reclaim orphaned workers that a previous
     * session left behind.
     */
    private static List<Long> findRunningPids(int workerId, int port) {
        Pattern pattern = Pattern.compile(
                "com\\.cs324\\.backend\\.worker\\.WorkerServer\\s+" + workerId + "\\s+" + port + "(?:\\s|$)");
        List<Long> pids = new ArrayList<>();
        try {
            ProcessHandle.allProcesses().forEach(handle -> {
                String commandLine = handle.info().commandLine().orElse("");
                if (commandLine.contains(WorkerServer.class.getName())
                        && pattern.matcher(commandLine).find()) {
                    pids.add(handle.pid());
                }
            });
        } catch (Exception e) {
            System.err.println("Could not enumerate running worker processes: " + e.getMessage());
        }
        return pids;
    }

    private static void status() {
        int running = 0;
        for (int workerId : WorkerClusterConfig.workerIds()) {
            Path pidFile = Paths.get(WorkerClusterConfig.LOG_DIR, pidFileName(workerId));
            Integer pid = readPid(pidFile);
            boolean alive = pid != null && isAlive(pid);
            if (alive) {
                running++;
            }
            System.out.println("Worker " + workerId
                    + " (port " + WorkerClusterConfig.portFor(workerId) + "): "
                    + (alive ? "RUNNING, pid " + pid : "not running"));
        }
        System.out.println(running + "/" + WorkerClusterConfig.WORKER_COUNT + " workers running");
    }

    /**
     * Resolves the classpath a spawned worker needs. The classes directory is
     * taken from this launcher's own code source (works under plain
     * {@code java -cp target/classes} and under {@code mvn exec:java}); the
     * gRPC/protobuf dependency jars come from {@code target/classpath.txt},
     * generated by the maven-dependency-plugin at {@code process-classes}.
     */
    private static String currentClasspath() {
        String classesDir;
        try {
            URI uri = WorkerClusterLauncher.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            classesDir = Paths.get(uri).toAbsolutePath().toString();
        } catch (Exception e) {
            classesDir = System.getProperty("java.class.path", "");
        }

        Path classpathFile = Paths.get("target", "classpath.txt");
        if (Files.exists(classpathFile)) {
            try {
                String deps = Files.readString(classpathFile, StandardCharsets.UTF_8).trim();
                if (!deps.isEmpty()) {
                    return classesDir + File.pathSeparator + deps;
                }
            } catch (IOException ignored) {
                // fall back to classes only
            }
        }
        return classesDir;
    }

    private static String logFileName(int workerId) {
        return "worker-" + workerId + ".log";
    }

    private static String pidFileName(int workerId) {
        return "worker-" + workerId + ".pid";
    }

    private static Integer readPid(Path pidFile) {
        try {
            if (!Files.exists(pidFile)) {
                return null;
            }
            String raw = Files.readString(pidFile, StandardCharsets.UTF_8).trim();
            return raw.isEmpty() ? null : Integer.valueOf(raw);
        } catch (IOException | NumberFormatException e) {
            return null;
        }
    }

    private static boolean isAlive(int pid) {
        return ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false);
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // best effort cleanup
        }
    }
}