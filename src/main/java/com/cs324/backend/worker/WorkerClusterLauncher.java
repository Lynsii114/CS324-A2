package com.cs324.backend.worker;

import com.cs324.backend.api.WorkerService;
import com.cs324.backend.bootstrap.BootstrapServer;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Launches {@code WorkerClusterConfig.WORKER_COUNT} worker nodes (default 6,
 * minimum 6) as independent JVM processes. Each process gets its own RMI
 * registry port, its own log file and its own pid file, so the workers stay
 * fully isolated while still talking to each other (and to the Bootstrap Node)
 * through Java RMI.
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
            if (isReachableOverRmi(workerId, bootstrapHost)) {
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
                    + ", rmi port " + port + ", log " + logDir.resolve(logFileName(workerId)) + ")");
        }

        if (!started.isEmpty()) {
            verifyOverRmi(started, WorkerClusterConfig.DEFAULT_HOST);
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
            Registry registry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            com.cs324.backend.api.BootstrapService bootstrap =
                    (com.cs324.backend.api.BootstrapService) registry.lookup(BootstrapServer.SERVICE_NAME);
            int expected = bootstrap.getClusterSize();
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
                    + " - start it first with 'java -cp target/classes "
                    + "com.cs324.backend.bootstrap.BootstrapServer [port] [workerCount]");
        }
    }

    /**
     * Polls each worker's own RMI registry until the worker is reachable.
     */
    private static void verifyOverRmi(List<Integer> workerIds, String host) {
        long deadline = System.currentTimeMillis() + LOOKUP_TIMEOUT_MS;
        List<Integer> pending = new ArrayList<>(workerIds);

        while (!pending.isEmpty() && System.currentTimeMillis() < deadline) {
            pending.removeIf(workerId -> {
                try {
                    Registry registry = LocateRegistry.getRegistry(host, WorkerClusterConfig.portFor(workerId));
                    WorkerService worker = (WorkerService) registry.lookup(WorkerClusterConfig.serviceNameFor(workerId));
                    return worker.getWorkerId() == workerId;
                } catch (Exception e) {
                    return false;
                }
            });
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
                System.err.println("Worker " + workerId + " did not answer its RMI lookup within "
                        + LOOKUP_TIMEOUT_MS + " ms - check its log file");
            } else {
                System.out.println("Verified over RMI: worker " + workerId
                        + " at rmi://" + host + ":" + WorkerClusterConfig.portFor(workerId)
                        + "/" + WorkerClusterConfig.serviceNameFor(workerId));
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

    /** Lightweight check that a worker currently answers its RMI registry with its own id. */
    private static boolean isReachableOverRmi(int workerId, String host) {
        try {
            Registry registry = LocateRegistry.getRegistry(host, WorkerClusterConfig.portFor(workerId));
            WorkerService worker = (WorkerService) registry.lookup(WorkerClusterConfig.serviceNameFor(workerId));
            return worker.getWorkerId() == workerId;
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
     * Resolves the classpath of the running project. Works both under
     * {@code mvn exec:java} (isolated classloader) and under a plain
     * {@code java -cp target/classes} launch.
     */
    private static String currentClasspath() {
        try {
            URI uri = WorkerClusterLauncher.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            return Paths.get(uri).toAbsolutePath().toString();
        } catch (Exception e) {
            return System.getProperty("java.class.path");
        }
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
