package com.cs324.backend.worker;

import com.cs324.common.GrpcSupport;
import com.cs324.common.WorkerClusterConfig;
import com.cs324.grpc.BootstrapServiceGrpc;
import com.cs324.grpc.Empty;
import com.cs324.grpc.WorkerServiceGrpc;
import io.grpc.ManagedChannel;

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
 * minimum 6) as independent JVM processes. Each process gets its own gRPC port,
 * its own log file and its own pid file, so the workers stay fully isolated
 * while still talking to each other (and to the Bootstrap Node) through gRPC.
 *
 * <p>Workers are spawned from the jar this launcher itself was loaded from, so
 * run the launcher from a built fat jar ({@code worker/target/worker.jar} or the
 * bootstrap's jar) rather than from a bare {@code target/classes} directory.
 *
 * Usage:
 *   WorkerClusterLauncher [start|stop|status] [bootstrapHost] [bootstrapPort] [workerCount]
 */
public final class WorkerClusterLauncher {

    private static final long LOOKUP_TIMEOUT_MS = 20_000;
    private static final long LOOKUP_POLL_MS = 500;
    private static final long RPC_DEADLINE_SECONDS = 3;

    private WorkerClusterLauncher() {
    }

    public static void main(String[] args) {
        String command = args.length > 0 ? args[0] : "start";
        // Apply an optional worker count BEFORE WorkerClusterConfig is first
        // loaded, so this JVM and every spawned worker agree on the cluster size.
        if (args.length > 3 && "start".equals(command)) {
            System.setProperty(WorkerClusterConfig.WORKER_COUNT_PROPERTY, args[3]);
        }
        String bootstrapHost = args.length > 1 ? args[1] : WorkerClusterConfig.DEFAULT_HOST;
        int bootstrapPort = args.length > 2 ? Integer.parseInt(args[2]) : WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT;

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

    public static List<Integer> start(String bootstrapHost, int bootstrapPort) {
        warnOnSizeMismatch(bootstrapHost, bootstrapPort);
        Path logDir = Paths.get(WorkerClusterConfig.LOG_DIR);
        try {
            Files.createDirectories(logDir);
        } catch (IOException e) {
            System.err.println("Could not create log directory " + logDir + ": " + e.getMessage());
            return new ArrayList<>();
        }

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
                alreadyRunning.add(workerId);
                List<Long> orphans = findRunningPids(workerId, port);
                if (!orphans.isEmpty()) {
                    writeQuietly(pidFile, String.valueOf(orphans.get(0)));
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
                    WorkerNode.class.getName(),
                    String.valueOf(workerId),
                    String.valueOf(port),
                    bootstrapHost,
                    String.valueOf(bootstrapPort),
                    WorkerClusterConfig.DEFAULT_HOST);
            builder.directory(workingDir.toFile());
            builder.redirectOutput(ProcessBuilder.Redirect.to(logDir.resolve(logFileName(workerId)).toFile()));
            builder.redirectError(ProcessBuilder.Redirect.appendTo(logDir.resolve(logFileName(workerId)).toFile()));

            try {
                Process process = builder.start();
                writeQuietly(pidFile, String.valueOf(process.pid()));
                started.add(workerId);
                System.out.println("Started Worker " + workerId + " (pid " + process.pid()
                        + ", grpc port " + port + ", log " + logDir.resolve(logFileName(workerId)) + ")");
            } catch (IOException e) {
                System.err.println("Could not start Worker " + workerId + ": " + e.getMessage());
            }
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
        ManagedChannel channel = null;
        try {
            channel = GrpcSupport.channel(bootstrapHost, bootstrapPort);
            BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap =
                    BootstrapServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
            int expected = bootstrap.getClusterSize(Empty.getDefaultInstance()).getValue();
            if (expected != WorkerClusterConfig.WORKER_COUNT) {
                System.err.println("WARNING: Bootstrap Node is configured for " + expected
                        + " worker node(s) but this launcher is configured for "
                        + WorkerClusterConfig.WORKER_COUNT
                        + ". Launch via 'BootstrapServer [port] [workerCount]' or pass a matching "
                        + "workerCount so the whole cluster agrees.");
            }
        } catch (Exception e) {
            System.err.println("Bootstrap Node not reachable at " + bootstrapHost + ":" + bootstrapPort
                    + " - start it first with 'java -jar bootstrap/target/bootstrap.jar [port] [workerCount]");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    /** Polls each worker's gRPC server until the worker answers with its own id. */
    private static void verifyOverGrpc(List<Integer> workerIds, String host) {
        long deadline = System.currentTimeMillis() + LOOKUP_TIMEOUT_MS;
        List<Integer> pending = new ArrayList<>(workerIds);

        while (!pending.isEmpty() && System.currentTimeMillis() < deadline) {
            pending.removeIf(workerId -> isReachableOverGrpc(workerId, host));
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
                        + " at grpc://" + host + ":" + WorkerClusterConfig.portFor(workerId));
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
     * alive - i.e. the worker was launched (and is being tracked) by this launcher.
     */
    public static boolean isManagedAndAlive(int workerId) {
        Path pidFile = Paths.get(WorkerClusterConfig.LOG_DIR, pidFileName(workerId));
        Integer pid = readPid(pidFile);
        return pid != null && isAlive(pid);
    }

    /** Lightweight check that a worker currently answers its gRPC port with its own id. */
    private static boolean isReachableOverGrpc(int workerId, String host) {
        ManagedChannel channel = null;
        try {
            channel = GrpcSupport.channel(host, WorkerClusterConfig.portFor(workerId));
            WorkerServiceGrpc.WorkerServiceBlockingStub worker = WorkerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
            return worker.getWorkerId(Empty.getDefaultInstance()).getValue() == workerId;
        } catch (Exception e) {
            return false;
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    /**
     * Finds every live JVM running our WorkerNode command line for <i>workerId</i>
     * on <i>port</i>, regardless of whether a pid file tracks it.
     */
    private static List<Long> findRunningPids(int workerId, int port) {
        Pattern pattern = Pattern.compile(
                "com\\.cs324\\.backend\\.worker\\.WorkerNode\\s+" + workerId + "\\s+" + port + "(?:\\s|$)");
        List<Long> pids = new ArrayList<>();
        try {
            ProcessHandle.allProcesses().forEach(handle -> {
                String commandLine = handle.info().commandLine().orElse("");
                if (commandLine.contains(WorkerNode.class.getName())
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
     * Resolves the classpath used to spawn workers. Prefers the jar this launcher
     * was loaded from (so a fat jar carries every dependency into each worker
     * JVM); falls back to the current JVM classpath when running from a
     * directory.
     */
    private static String currentClasspath() {
        try {
            URI uri = WorkerClusterLauncher.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            Path path = Paths.get(uri).toAbsolutePath();
            if (Files.isRegularFile(path) && path.toString().endsWith(".jar")) {
                return path.toString();
            }
        } catch (Exception ignored) {
            // fall through to the JVM classpath
        }
        return System.getProperty("java.class.path");
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

    private static void writeQuietly(Path path, String value) {
        try {
            Files.writeString(path, value, StandardCharsets.UTF_8);
        } catch (IOException ignored) {
            // best effort
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
            // best effort cleanup
        }
    }
}
