package com.cs324.common;

/**
 * Central configuration shared by all three deployables - the Bootstrap Node,
 * the worker nodes and the client. The cluster size is configurable: by default
 * six worker nodes (IDs 1..6), and extra nodes can be added on top by setting
 * the {@code cs324.worker.count} system property - the minimum is 6, so a
 * smaller value is ignored. Each worker owns its own gRPC port (5001 + id - 1)
 * hosting both its {@code ClusterService} and {@code ClientService}, so every
 * worker runs in an isolated environment.
 *
 * <p>The {@code cs324.worker.count} property must be identical on every JVM in
 * the cluster (the Bootstrap Node and each worker), because it drives the
 * cluster-complete and startup-lottery thresholds.</p>
 */
public final class ClusterConfig {

    /** System property used to pick the number of worker nodes (minimum 6). */
    public static final String WORKER_COUNT_PROPERTY = "cs324.worker.count";

    /**
     * Directory (relative to the directory the worker launcher/server is started
     * from - by convention the repository root) holding the per-worker
     * {@code worker-<id>.log} / {@code worker-<id>.pid} files.
     */
    public static final String LOG_DIR = "logs";

    /** The smallest cluster that can be started. */
    public static final int MIN_WORKER_COUNT = 6;

    /** Cluster size used when {@code cs324.worker.count} is not set on the command line. */
    public static final int DEFAULT_WORKER_COUNT = 6;

    /** Number of worker nodes in the cluster (never below {@link #MIN_WORKER_COUNT}). */
    public static final int WORKER_COUNT = resolveWorkerCount();

    public static final int FIRST_WORKER_ID = 1;
    public static final int LAST_WORKER_ID = FIRST_WORKER_ID + WORKER_COUNT - 1;
    public static final int BASE_WORKER_PORT = 5001;
    public static final String DEFAULT_HOST = "localhost";

    // ---- fault tolerance knobs ----

    /** Interval between worker -> bootstrap heartbeats. */
    public static final long HEARTBEAT_PERIOD_MS = 2_000;

    /** A worker whose heartbeat is older than this is considered offline by the bootstrap. */
    public static final long HEARTBEAT_TIMEOUT_MS = 8_000;

    /** Deadline used when a worker probes the coordinator for liveness. */
    public static final long COORDINATOR_PROBE_TIMEOUT_MS = 3_000;

    /** How often workers check (and elect / fail over) the coordinator. */
    public static final long COORDINATOR_MONITOR_PERIOD_MS = 4_000;

    /** Deadline for a single worker->worker segment RPC; violation = worker deemed failed. */
    public static final long SEGMENT_CALL_TIMEOUT_MS = 15_000;

    /** How many replacement attempts a coordinator makes for one failed segment. */
    public static final int MAX_SEGMENT_RETRIES = 3;

    private ClusterConfig() {
    }

    /** Reads {@code cs324.worker.count} (clamped to the 6-node minimum) at class load. */
    public static int resolveWorkerCount() {
        String raw = System.getProperty(WORKER_COUNT_PROPERTY);
        if (raw == null) {
            return DEFAULT_WORKER_COUNT;
        }
        try {
            return Math.max(MIN_WORKER_COUNT, Integer.parseInt(raw.trim()));
        } catch (NumberFormatException e) {
            return DEFAULT_WORKER_COUNT;
        }
    }

    /** Returns the configured worker IDs: 1, 2, ..., WORKER_COUNT. */
    public static int[] workerIds() {
        int[] ids = new int[WORKER_COUNT];
        for (int i = 0; i < WORKER_COUNT; i++) {
            ids[i] = FIRST_WORKER_ID + i;
        }
        return ids;
    }

    /** Maps a worker ID to its dedicated gRPC port (1 -> 5001 ... N -> 5000+N).
     *  The same port hosts the worker's ClusterService and ClientService. */
    public static int portFor(int workerId) {
        if (workerId < FIRST_WORKER_ID || workerId > LAST_WORKER_ID) {
            throw new IllegalArgumentException(
                    "workerId must be between " + FIRST_WORKER_ID + " and " + LAST_WORKER_ID + ", got " + workerId);
        }
        return BASE_WORKER_PORT + (workerId - FIRST_WORKER_ID);
    }
}