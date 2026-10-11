package com.cs324.backend.worker;

/**
 * Central configuration for the worker cluster. The cluster size is
 * configurable: by default six worker nodes (IDs 1..6), and extra nodes can be
 * added on top by setting the {@code cs324.worker.count} system property - the
 * minimum is 6, so a smaller value is ignored. Each worker owns its own RMI
 * registry port (5001 + id - 1), so every worker runs in an isolated
 * environment.
 */
public final class WorkerClusterConfig {

    /** System property used to pick the number of worker nodes (minimum 6). */
    public static final String WORKER_COUNT_PROPERTY = "cs324.worker.count";

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
    public static final String LOG_DIR = "logs";

    private WorkerClusterConfig() {
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

    /** Maps a worker ID to its dedicated RMI registry port (1 -> 5001 ... N -> 5000+N). */
    public static int portFor(int workerId) {
        if (workerId < FIRST_WORKER_ID || workerId > LAST_WORKER_ID) {
            throw new IllegalArgumentException(
                    "workerId must be between " + FIRST_WORKER_ID + " and " + LAST_WORKER_ID + ", got " + workerId);
        }
        return BASE_WORKER_PORT + (workerId - FIRST_WORKER_ID);
    }

    /** RMI binding name of a worker inside its own registry. */
    public static String serviceNameFor(int workerId) {
        return WorkerServer.SERVICE_NAME_PREFIX + workerId;
    }
}
