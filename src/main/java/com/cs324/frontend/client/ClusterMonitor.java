package com.cs324.frontend.client;

import com.cs324.backend.grpc.GrpcChannels;
import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.proto.Empty;
import com.cs324.proto.WorkerInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Background health monitor that drives the client dashboard.
 *
 * <p>Every {@link #PROBE_INTERVAL_MS} ms it probes (on its own daemon thread,
 * never on the JavaFX application thread):</p>
 * <ul>
 *   <li>the <b>Bootstrap Node</b> (server) - a failed probe marks the first
 *       <b>failure detection</b>; repeated failures move to
 *       <b>reconnecting</b>; a successful probe after a failure reports
 *       <b>recovery</b> (with the outage duration);</li>
 *   <li>the <b>coordinator worker</b> - unreachable coordinator or a changed
 *       coordinator id is reported as a coordinator failure/failover event;</li>
 *   <li>the <b>worker membership</b> - online/offline counts and per-worker
 *       status for the dashboard.</li>
 * </ul>
 *
 * <p>All callbacks are invoked on the monitor thread; the GUI is responsible
 * for marshalling them onto the JavaFX application thread.</p>
 */
public final class ClusterMonitor {

    /** How often the health probes run. */
    public static final long PROBE_INTERVAL_MS = 2_000;
    /** Deadline for a single probe RPC - a violation counts as a failure. */
    public static final long PROBE_DEADLINE_MS = 1_500;

    /** Lifecycle of the Bootstrap Node ("server") as seen by the client. */
    public enum ServerState {
        /** Not monitoring yet (never connected, or disconnected). */
        OFFLINE,
        /** Server answering probes - system is running. */
        RUNNING,
        /** First failed probe - failure detected. */
        FAILURE_DETECTED,
        /** Still down - client keeps retrying at the probe interval. */
        RECONNECTING,
        /** Server answered again after an outage - recovery reported. */
        RECOVERED
    }

    /** Lifecycle of the elected coordinator worker. */
    public enum CoordinatorState {
        UNKNOWN,
        /** Coordinator answering probes. */
        ONLINE,
        /** Coordinator unreachable - failover election expected. */
        FAILURE_DETECTED,
        /** A (new) coordinator answered after a failure or term change. */
        RECOVERED
    }

    /** Severity of a dashboard timeline entry. */
    public enum Severity { INFO, SUCCESS, WARNING, ERROR }

    /** Callbacks delivered on the monitor thread. */
    public interface Listener {
        void onServerState(ServerState state, String detail, int consecutiveFailures, long outageMs);

        void onCoordinatorState(CoordinatorState state, String detail);

        void onWorkersSnapshot(int online, int total, List<WorkerInfo> workers);

        void onEvent(Severity severity, String message);
    }

    private final Listener listener;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "cluster-health-monitor");
        t.setDaemon(true);
        return t;
    });

    private volatile String host = "localhost";
    private volatile int port = com.cs324.backend.bootstrap.BootstrapServer.DEFAULT_PORT;
    private volatile boolean active;

    // Server-side probe state.
    private ServerState serverState = ServerState.OFFLINE;
    private int consecutiveFailures;
    private long outageStartedAt;
    private long lastSuccessAt;

    // Coordinator probe state (target supplied by the GUI after resolving it).
    private volatile String coordinatorHost;
    private volatile int coordinatorPort = -1;
    private volatile int coordinatorId = -1;
    private CoordinatorState coordinatorState = CoordinatorState.UNKNOWN;
    private boolean coordinatorProbeFailed;

    private ScheduledFuture<?> task;

    public ClusterMonitor(Listener listener) {
        this.listener = listener;
    }

    /** Points the monitor at the Bootstrap Node and starts probing. */
    public synchronized void start(String host, int port) {
        this.host = host;
        this.port = port;
        this.active = true;
        this.consecutiveFailures = 0;
        this.outageStartedAt = 0;
        this.serverState = ServerState.OFFLINE;
        if (task == null || task.isCancelled()) {
            task = scheduler.scheduleWithFixedDelay(this::probeSafely,
                    0, PROBE_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }

    public synchronized void stop() {
        active = false;
        if (task != null) {
            task.cancel(false);
            task = null;
        }
        serverState = ServerState.OFFLINE;
        coordinatorState = CoordinatorState.UNKNOWN;
    }

    /** Updates the coordinator target; called by the GUI whenever it (re-)resolves one. */
    public void setCoordinator(int id, String host, int port) {
        this.coordinatorId = id;
        this.coordinatorHost = host;
        this.coordinatorPort = port;
        this.coordinatorProbeFailed = false;
    }

    public void clearCoordinator() {
        this.coordinatorId = -1;
        this.coordinatorHost = null;
        this.coordinatorPort = -1;
        this.coordinatorState = CoordinatorState.UNKNOWN;
        this.coordinatorProbeFailed = false;
    }

    public ServerState serverState() {
        return serverState;
    }

    // ---------------------------------------------------------------- probing

    private void probeSafely() {
        if (!active) {
            return;
        }
        try {
            probe();
        } catch (RuntimeException e) {
            // Never let an unexpected exception kill the scheduled probe task.
            reportServerFailure(e.getClass().getSimpleName());
        }
    }

    private void probe() {
        // 1) Bootstrap Node ("server") probe.
        try {
            GrpcChannels.bootstrap(host, port)
                    .withDeadlineAfter(PROBE_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .getClusterSize(Empty.getDefaultInstance());
            reportServerSuccess();
        } catch (Exception e) {
            reportServerFailure(rootMessage(e));
            return; // server down: skip membership/coordinator probes this cycle
        }

        // 2) Worker membership snapshot.
        try {
            List<WorkerInfo> workers = GrpcChannels.bootstrap(host, port)
                    .withDeadlineAfter(PROBE_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .getActiveWorkers(Empty.getDefaultInstance())
                    .getWorkersList();
            List<WorkerInfo> sorted = new ArrayList<>(workers);
            sorted.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
            int online = 0;
            for (WorkerInfo w : sorted) {
                if (w.getOnline()) {
                    online++;
                }
            }
            int expected = expectedClusterSize(workers.size());
            listener.onWorkersSnapshot(online, expected, sorted);
        } catch (Exception e) {
            // membership probe failure alone is not a server failure; note it
            listener.onEvent(Severity.WARNING,
                    "Membership probe failed: " + rootMessage(e));
        }

        // 3) Coordinator probe (only when a target is known).
        probeCoordinator();
    }

    private int probeCoordinator() {
        String cHost = coordinatorHost;
        int cPort = coordinatorPort;
        if (cHost == null || cPort < 0) {
            return 0;
        }
        try {
            int reported = GrpcChannels.cluster(cHost, cPort)
                    .withDeadlineAfter(PROBE_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .getCurrentCoordinatorId(Empty.getDefaultInstance())
                    .getValue();
            if (coordinatorProbeFailed) {
                coordinatorProbeFailed = false;
                coordinatorState = CoordinatorState.RECOVERED;
                listener.onCoordinatorState(CoordinatorState.RECOVERED,
                        "worker " + coordinatorId + " responding again");
                listener.onEvent(Severity.SUCCESS,
                        "COORDINATOR RECOVERED - worker " + coordinatorId + " answering again");
            } else if (reported != coordinatorId && coordinatorState != CoordinatorState.UNKNOWN) {
                listener.onEvent(Severity.WARNING,
                        "Coordinator changed: worker " + coordinatorId + " -> worker " + reported);
            }
            return reported;
        } catch (Exception e) {
            if (!coordinatorProbeFailed) {
                coordinatorProbeFailed = true;
                coordinatorState = CoordinatorState.FAILURE_DETECTED;
                listener.onCoordinatorState(CoordinatorState.FAILURE_DETECTED,
                        "worker " + coordinatorId + " unreachable (" + rootMessage(e) + ")");
                listener.onEvent(Severity.ERROR,
                        "COORDINATOR FAILURE DETECTED - worker " + coordinatorId
                                + " unreachable; failover election expected");
            }
            return -1;
        }
    }

    // ----------------------------------------------------------- state reports

    private void reportServerSuccess() {
        long now = System.currentTimeMillis();
        lastSuccessAt = now;
        ServerState previous = serverState;
        if (serverState == ServerState.FAILURE_DETECTED
                || serverState == ServerState.RECONNECTING) {
            long outageMs = now - outageStartedAt;
            int attempts = Math.max(consecutiveFailures, 1);
            serverState = ServerState.RECOVERED;
            consecutiveFailures = 0;
            outageStartedAt = 0;
            listener.onServerState(ServerState.RECOVERED,
                    "recovered after " + formatDuration(outageMs)
                            + " (" + attempts + " failed probe(s))", 0, outageMs);
            listener.onEvent(Severity.SUCCESS,
                    "SERVER RECOVERED - Bootstrap Node reachable again after "
                            + formatDuration(outageMs) + ", " + attempts + " reconnect attempt(s)");
        } else if (serverState != ServerState.RUNNING) {
            serverState = ServerState.RUNNING;
            listener.onServerState(ServerState.RUNNING, "all probes passing", 0, 0);
            if (previous == ServerState.OFFLINE) {
                listener.onEvent(Severity.SUCCESS,
                        "SERVER RUNNING - Bootstrap Node at " + host + ":" + port + " healthy");
            }
        } else {
            listener.onServerState(ServerState.RUNNING, "all probes passing", 0, 0);
        }
    }

    private void reportServerFailure(String reason) {
        long now = System.currentTimeMillis();
        if (serverState == ServerState.RUNNING
                || serverState == ServerState.RECOVERED
                || serverState == ServerState.OFFLINE) {
            // Failure detection: first failed probe after a healthy period.
            serverState = ServerState.FAILURE_DETECTED;
            consecutiveFailures = 1;
            outageStartedAt = now;
            listener.onServerState(ServerState.FAILURE_DETECTED,
                    "failure detected: " + reason, 1, 0);
            listener.onEvent(Severity.ERROR,
                    "SERVER FAILURE DETECTED - probe to " + host + ":" + port
                            + " failed (" + reason + ")");
        } else {
            consecutiveFailures++;
            serverState = ServerState.RECONNECTING;
            long outageMs = now - outageStartedAt;
            listener.onServerState(ServerState.RECONNECTING,
                    "reconnect attempt " + consecutiveFailures + ": " + reason,
                    consecutiveFailures, outageMs);
            listener.onEvent(Severity.WARNING,
                    "SERVER RECONNECTING - attempt " + consecutiveFailures
                            + " to " + host + ":" + port
                            + " (" + formatDuration(outageMs) + " since failure)");
        }
    }

    // ---------------------------------------------------------------- helpers

    private int expectedClusterSize(int registered) {
        return Math.max(Math.max(registered, WorkerClusterConfig.WORKER_COUNT), 1);
    }

    static String formatDuration(long ms) {
        if (ms < 1_000) {
            return ms + " ms";
        }
        double seconds = ms / 1_000.0;
        if (seconds < 90) {
            return String.format("%.1f s", seconds);
        }
        long minutes = (long) (seconds / 60);
        return minutes + " m " + Math.round(seconds - minutes * 60) + " s";
    }

    static String rootMessage(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        if (current instanceof StatusRuntimeException sre) {
            Status status = sre.getStatus();
            String desc = status.getDescription();
            return status.getCode() + (desc == null ? "" : " (" + desc + ")");
        }
        String message = current.getMessage();
        return message == null ? current.getClass().getSimpleName() : message;
    }

    /** Shuts the monitor down (called when the GUI closes). */
    public void shutdown() {
        stop();
        scheduler.shutdownNow();
    }
}
