package com.cs324.backend.worker;

import com.cs324.common.ClusterConfig;
import com.cs324.common.GrpcChannels;
import com.cs324.proto.BootstrapServiceGrpc;
import com.cs324.proto.CandidateInfo;
import com.cs324.proto.ClusterServiceGrpc;
import com.cs324.proto.ElectionMessage;
import com.cs324.proto.Empty;
import com.cs324.proto.ProcessedParticipants;
import com.cs324.proto.SegmentUpdate;
import com.cs324.proto.TaskLedgerEntry;
import com.cs324.proto.TaskStatus;
import com.cs324.proto.TaskUpdate;
import com.cs324.proto.TermEndNotification;
import com.cs324.proto.WinnerAnnouncement;
import com.cs324.proto.WorkerIdRequest;
import com.cs324.proto.WorkerInfo;
import com.cs324.proto.WorkerProgress;
import com.cs324.proto.WorkerState;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DistriLab worker node: the refactored gRPC counterpart of the legacy RMI
 * {@code WorkerServiceImpl}. Hosts the worker state machine shared by the
 * {@code ClusterService} and {@code ClientService} gRPC services:
 * <ul>
 *   <li>Phase 1 - startup timer lottery claims startup priorities N..1 from the
 *       Bootstrap Node; the priority-N worker declares itself the initial
 *       coordinator.</li>
 *   <li>Phase 2 - the elected coordinator splits client jobs into contiguous
 *       segments, runs them over the least-loaded worker nodes and streams
 *       {@link TaskUpdate} progress to the client.</li>
 *   <li>Phase 3 - after 5 jobs a term ends; a lowest-JAC re-election rotates the
 *       coordinator.</li>
 *   <li>Fault tolerance - heartbeats to the Bootstrap Node, a coordinator
 *       liveness monitor with automatic failover election, per-segment retry /
 *       reassignment on worker death, dynamic joining mid-job, and a replicated
 *       task ledger that lets a new coordinator reconstruct in-flight client
 *       jobs without a restart.</li>
 * </ul>
 */
public class WorkerNode {

    /** Sentinel used when no coordinator has been elected yet. */
    public static final int NO_COORDINATOR = -1;

    /** A coordinator may process at most this many submitted jobs during one term. */
    public static final int COORDINATOR_TERM_LIMIT = 5;

    private static final int SEGMENT_NUMBERS = 3;          // MAX / PRIMECOUNT numbers per segment
    private static final int SEGMENT_RANGE_LENGTH = 200;   // PRIMESUM range length per segment

    private static final long LOTTERY_MIN_DELAY_MS = 150;
    private static final int LOTTERY_SPAN_MS = 150;
    private static final long LOTTERY_POLL_MS = 200;
    private static final long LOTTERY_TIMEOUT_MS = 20_000;
    private static final long AVAILABLE_LOTTERY_TIMEOUT_MS = 20_000;

    private static final long FIRST_WORKER_DELAY_MS = 2_500;
    private static final long WORKER_DELAY_STEP_MS = 2_000;

    private static final int MAX_TASK_HISTORY = 50;

    private final int workerId;
    private final String registeredHost;
    private final int registeredPort;
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;

    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final AtomicInteger currentCoordinatorId = new AtomicInteger(NO_COORDINATOR);
    private final AtomicInteger jobsThisTerm = new AtomicInteger(0);
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicInteger jobThreadCounter = new AtomicInteger(0);

    private final Set<String> processedElectionIds = ConcurrentHashMap.newKeySet();
    private final Set<WorkerInfo> neighbours = ConcurrentHashMap.newKeySet();
    private final Deque<TaskUpdate> taskHistory = new ConcurrentLinkedDeque<>();

    /** Overwritten per background tick; used to fetch worker state. */
    private volatile int priorityId = 0;
    private volatile long lotteryFireAt = -1L;
    private volatile boolean declaredInitial = false;
    private volatile boolean neighboursSyncedAfterLottery = false;
    /** Last observed online membership; a change triggers a neighbour-ring rebuild. */
    private volatile Set<Integer> lastRingOnlineIds = java.util.Collections.emptySet();
    private final long lotteryDeadline = System.currentTimeMillis() + LOTTERY_TIMEOUT_MS;

    private final ScheduledExecutorService lotteryScheduler;
    private final ScheduledExecutorService electionScheduler;
    private final ScheduledExecutorService heartbeatScheduler;
    private final ExecutorService jobExecutor;

    public WorkerNode(int workerId, String host, int port, String bootstrapHost, int bootstrapPort) {
        this.workerId = workerId;
        this.registeredHost = host;
        this.registeredPort = port;
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.bootstrap = GrpcChannels.bootstrap(bootstrapHost, bootstrapPort);
        this.electionScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> namedDaemon(r, "coordinator-monitor-" + workerId));
        this.lotteryScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> namedDaemon(r, "startup-lottery-" + workerId));
        this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(
                r -> namedDaemon(r, "heartbeat-" + workerId));
        this.jobExecutor = Executors.newFixedThreadPool(
                Math.max(COORDINATOR_TERM_LIMIT + 1, Runtime.getRuntime().availableProcessors()),
                r -> namedDaemon(r, "job-" + workerId + "-" + jobThreadCounter.incrementAndGet()));
    }

    private static Thread namedDaemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    public int getWorkerId() {
        return workerId;
    }

    /** Stops every background scheduler and the job executor for a clean shutdown. */
    public void shutdown() {
        lotteryScheduler.shutdownNow();
        electionScheduler.shutdownNow();
        heartbeatScheduler.shutdownNow();
        jobExecutor.shutdownNow();
    }

    public int getRegisteredPort() {
        return registeredPort;
    }

    public String getRegisteredHost() {
        return registeredHost;
    }

    public WorkerInfo selfInfo() {
        return WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(registeredHost)
                .setPort(registeredPort)
                .setOnline(true)
                .build();
    }

    // ------------------------------------------------------------ state getters
    // (back the ClusterService RPCs)

    public WorkerState getWorkerState() {
        WorkerState.Builder b = WorkerState.newBuilder()
                .setWorkerId(workerId)
                .setHost(registeredHost)
                .setPort(registeredPort)
                .setPriorityId(priorityId)
                .setJac(jobAllocationCounter.get())
                .setCurrentCoordinatorId(currentCoordinatorId.get())
                .setJobsThisTerm(jobsThisTerm.get())
                .setOnline(bootstrapOnline());
        List<WorkerInfo> neighboursCopy = new ArrayList<>(neighbours);
        neighboursCopy.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
        b.addAllNeighbours(neighboursCopy);
        return b.build();
    }

    public int getPriorityId() {
        return priorityId;
    }

    public int getJobAllocationCounter() {
        return jobAllocationCounter.get();
    }

    public int incrementJobAllocationCounter() {
        return jobAllocationCounter.incrementAndGet();
    }

    public int recordAllocation() {
        int updated = jobAllocationCounter.incrementAndGet();
        System.out.println("[Worker " + workerId + "] JAC changed to " + updated
                + " (processed a distributed job segment)");
        return updated;
    }

    public int resetJobAllocationCounter() {
        jobAllocationCounter.set(0);
        return jobAllocationCounter.get();
    }

    public int getJobsThisTerm() {
        return jobsThisTerm.get();
    }

    public List<WorkerInfo> getNeighbours() {
        return new ArrayList<>(neighbours);
    }

    public void addNeighbour(WorkerInfo worker) {
        if (worker == null || worker.getWorkerId() < 1 || worker.getWorkerId() == workerId) {
            return;
        }
        neighbours.add(worker);
    }

    public void addNeighbour(int targetId) {
        List<WorkerInfo> active = safeActiveWorkers();
        for (WorkerInfo info : active) {
            if (info.getWorkerId() == targetId) {
                neighbours.add(info);
                return;
            }
        }
        System.err.println("[Worker " + workerId + "] cannot resolve worker " + targetId
                + " via Bootstrap Node - it is not registered");
    }

    public void removeNeighbour(int targetId) {
        neighbours.removeIf(w -> w.getWorkerId() == targetId);
    }

    public synchronized int syncNeighbours() {
        try {
            List<WorkerInfo> active = safeOnlineWorkers();
            boolean foundSelf = active.stream().anyMatch(i -> i.getWorkerId() == workerId);
            if (!foundSelf) {
                active.add(selfInfo());
            }
            active.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));

            neighbours.clear();
            if (active.size() <= 1) {
                return 0;
            }

            List<WorkerInfo> others = new ArrayList<>(active);
            others.removeIf(i -> i.getWorkerId() == workerId);

            // Ring backbone: predecessor + successor guarantee reachability.
            int idx = 0;
            for (int i = 0; i < active.size(); i++) {
                if (active.get(i).getWorkerId() == workerId) {
                    idx = i;
                    break;
                }
            }
            WorkerInfo predecessor = active.get((idx - 1 + active.size()) % active.size());
            if (predecessor.getWorkerId() != workerId) {
                neighbours.add(predecessor);
            }
            WorkerInfo successor = active.get((idx + 1) % active.size());
            if (successor.getWorkerId() != workerId) {
                neighbours.add(successor);
            }

            // Unstructured extra link: random peer.
            try {
                WorkerInfo randomPeer = bootstrap.getRandomWorker(Empty.getDefaultInstance());
                if (randomPeer != null && randomPeer.getWorkerId() < 1) {
                    randomPeer = null;
                }
                if (randomPeer != null && randomPeer.getWorkerId() != workerId
                        && neighbours.add(randomPeer)) {
                    System.out.println("[Worker " + workerId + "] random link added -> "
                            + randomPeer.getWorkerId() + " (neighbours=" + neighbours.size() + ")");
                }
            } catch (StatusRuntimeException quiet) {
                // bootstrap unreachable - keep the ring we have
            }
            return neighbours.size();
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] neighbour sync failed: " + e.getMessage());
            return neighbours.size();
        }
    }

    public int getCurrentCoordinatorId() {
        return currentCoordinatorId.get();
    }

    public void setCurrentCoordinatorId(int coordinatorId) {
        recordCoordinator(coordinatorId);
    }

    private void recordCoordinator(int coordinatorId) {
        int previous = currentCoordinatorId.getAndSet(coordinatorId);
        if (coordinatorId == workerId) {
            jobsThisTerm.set(0);
        }
        System.out.println("[Worker " + workerId + "] coordinator set to " + coordinatorId
                + (previous == coordinatorId ? "" : " (was " + previous + ")"));
    }

    // ----------------------------------------------------------- background tasks

    public void startBackgroundServices() {
        long initialDelay = Math.max(FIRST_WORKER_DELAY_MS, (long) workerId * WORKER_DELAY_STEP_MS);
        electionScheduler.scheduleWithFixedDelay(this::coordinatorMonitoredElectionTick,
                initialDelay, ClusterConfig.COORDINATOR_MONITOR_PERIOD_MS, TimeUnit.MILLISECONDS);
        lotteryScheduler.scheduleWithFixedDelay(this::lotteryTick,
                LOTTERY_MIN_DELAY_MS, LOTTERY_POLL_MS, TimeUnit.MILLISECONDS);
        heartbeatScheduler.scheduleWithFixedDelay(this::heartbeatTick,
                ClusterConfig.HEARTBEAT_PERIOD_MS, ClusterConfig.HEARTBEAT_PERIOD_MS,
                TimeUnit.MILLISECONDS);
    }

    private void heartbeatTick() {
        try {
            bootstrap.heartbeat(WorkerIdRequest.newBuilder().setWorkerId(workerId).build());
            maybeResyncRingAfterMembershipChange();
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] heartbeat to bootstrap failed: "
                    + e.getStatus().getCode());
        }
    }

    /**
     * Rebuilds the neighbour ring whenever the bootstrap's online membership
     * changes (a worker joined or permanently disappeared). Without this, a
     * worker that joins a running cluster never becomes a neighbour of the
     * coordinator and would be invisible to segment assignment, because the ring
     * is otherwise only rebuilt during lottery/election steps.
     */
    private void maybeResyncRingAfterMembershipChange() {
        Set<Integer> current = new HashSet<>();
        for (WorkerInfo w : safeOnlineWorkers()) {
            current.add(w.getWorkerId());
        }
        if (current.equals(lastRingOnlineIds)) {
            return;
        }
        lastRingOnlineIds = current;
        if (!current.isEmpty()) {
            int count = syncNeighbours();
            System.out.println("[Worker " + workerId
                    + "] membership changed -> rebuilt neighbour ring (neighbours=" + count + ")");
        }
    }

    /**
     * Combines the coordinator liveness monitor with the legacy auto-election
     * check. First probes the current coordinator (if any): a failed probe means
     * the leader went offline, so the local view is demoted and a failover
     * election starts. If no coordinator is present, an election runs to either
     * elect the initial coordinator or replace a dead one.
     */
    private void coordinatorMonitoredElectionTick() {
        int coord = currentCoordinatorId.get();
        if (coord != NO_COORDINATOR && coord != workerId) {
            if (coordinatorResponsive(coord)) {
                return;
            }
            System.err.println("[Worker " + workerId + "] coordinator worker " + coord
                    + " is unreachable - demoting local view and starting a failover election");
            currentCoordinatorId.set(NO_COORDINATOR);
        }
        if (currentCoordinatorId.get() != NO_COORDINATOR || electionInProgress.get()) {
            return;
        }
        if (priorityId <= 0) {
            // A worker that joins AFTER the cluster is already running has no startup-priority
            // slot available (the bootstrap mints it a negative priority), so it can never win
            // the lottery-based election and would otherwise stay coordinator-less forever.
            // Adopt the existing coordinator from any peer that can report a live one.
            if (adoptCoordinatorFromPeers()) {
                return;
            }
            return; // nothing to adopt yet - retry on the next monitor tick
        }
        if (System.currentTimeMillis() < lotteryDeadline && !isPriorityTableComplete()) {
            return; // Phase 1 still running
        }
        try {
            initiateElection();
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] auto-election failed: " + e.getStatus().getCode());
        }
    }

    private boolean coordinatorResponsive(int coordWorkerId) {
        WorkerInfo info = lookupRegisteredWorker(coordWorkerId);
        if (info == null) {
            return false;
        }
        try {
            ClusterServiceGrpc.ClusterServiceBlockingStub stub =
                    GrpcChannels.cluster(info.getHost(), info.getPort())
                            .withDeadlineAfter(ClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS,
                                    TimeUnit.MILLISECONDS);
            WorkerState state = stub.getWorkerState(Empty.getDefaultInstance());
            return state.getWorkerId() == coordWorkerId;
        } catch (StatusRuntimeException e) {
            return false;
        }
    }

    /**
     * Late-joiner coordinator adoption: asks every reachable peer which worker it
     * currently accepts as coordinator and, once a live candidate is confirmed,
     * records it locally so a worker that joined after the election can serve and
     * coordinate jobs immediately.
     */
    private boolean adoptCoordinatorFromPeers() {
        for (WorkerInfo peer : safeOnlineWorkers()) {
            if (peer.getWorkerId() == workerId) {
                continue;
            }
            try {
                ClusterServiceGrpc.ClusterServiceBlockingStub stub =
                        GrpcChannels.cluster(peer.getHost(), peer.getPort())
                                .withDeadlineAfter(ClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS,
                                        TimeUnit.MILLISECONDS);
                int reported = stub.getCurrentCoordinatorId(Empty.getDefaultInstance()).getValue();
                if (reported != NO_COORDINATOR
                        && reported != workerId
                        && coordinatorResponsive(reported)) {
                    recordCoordinator(reported);
                    System.out.println("[Worker " + workerId + "] adopted existing coordinator worker "
                            + reported + " from peer " + peer.getWorkerId() + " (late join)");
                    return true;
                }
            } catch (StatusRuntimeException quiet) {
                // peer unreachable - try the next one
            }
        }
        return false;
    }

    // ------------------------------------------------------------- Phase 1 lottery

    private void lotteryTick() {
        if (declaredInitial) {
            return;
        }
        try {
            if (priorityId == 0) {
                if (lotteryFireAt == -1L) {
                    long completeTime = readClusterCompleteTime();
                    boolean fullCluster = readActiveWorkerCount() >= ClusterConfig.WORKER_COUNT;
                    if (completeTime > 0 && fullCluster) {
                        lotteryFireAt = completeTime + lotteryCountdown();
                        System.out.println("[Worker " + workerId + "] lottery countdown -> "
                                + (lotteryFireAt - System.currentTimeMillis())
                                + " ms (cluster complete at " + completeTime + ")");
                    } else if (System.currentTimeMillis() > lotteryDeadline) {
                        lotteryFireAt = System.currentTimeMillis() + lotteryCountdown();
                        System.out.println("[Worker " + workerId + "] cluster not complete within "
                                + AVAILABLE_LOTTERY_TIMEOUT_MS + " ms - starting a partial startup lottery");
                    } else {
                        return;
                    }
                }
                if (System.currentTimeMillis() < lotteryFireAt) {
                    return;
                }
                int assigned = bootstrap.lotteryClaim(
                        WorkerIdRequest.newBuilder().setWorkerId(workerId).build()).getPriority();
                priorityId = assigned;
                System.out.println("[Worker " + workerId + "] lottery claim -> startup priority " + assigned);
            }

            if (priorityId != 0 && isPriorityTableComplete() && !neighboursSyncedAfterLottery) {
                neighboursSyncedAfterLottery = true;
                syncNeighbours();
                System.out.println("[Worker " + workerId + "] rebuilt neighbour ring for the full cluster "
                        + "(neighbours=" + neighbours.size() + ")");
            }

            if (priorityId == ClusterConfig.WORKER_COUNT
                    && isPriorityTableComplete()
                    && currentCoordinatorId.get() == NO_COORDINATOR) {
                // The priority-N worker declares itself the initial coordinator. The
                // neighbour ring must exist BEFORE the declaration: gather it here too,
                // because the standalone sync above races with the last lottery claim
                // (its own bootstrap round-trip may see 5/6 priorities just before the
                // 6th claim lands, which would leave this coordinator ringless).
                if (!neighboursSyncedAfterLottery) {
                    neighboursSyncedAfterLottery = true;
                    syncNeighbours();
                    System.out.println("[Worker " + workerId + "] rebuilt neighbour ring for the full cluster "
                            + "(neighbours=" + neighbours.size() + ")");
                }
                declaredInitial = true;
                recordCoordinator(workerId);
                System.out.println("[Worker " + workerId + "] priority " + priorityId
                        + " -> declaring self Initial Coordinator and alerting the other "
                        + (ClusterConfig.WORKER_COUNT - 1) + " workers");
                for (WorkerInfo other : safeOnlineWorkers()) {
                    if (other.getWorkerId() == workerId) {
                        continue;
                    }
                    callSetCoordinator(other, workerId);
                }
            }
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] lottery check failed: "
                    + (e instanceof StatusRuntimeException sre ? sre.getStatus().getCode() : e.getMessage()));
        }
    }

    private static long lotteryCountdown() {
        return LOTTERY_MIN_DELAY_MS + ThreadLocalRandom.current().nextInt(LOTTERY_SPAN_MS + 1);
    }

    private long readClusterCompleteTime() {
        try {
            return bootstrap.getClusterCompleteTime(Empty.getDefaultInstance()).getEpochMillis();
        } catch (StatusRuntimeException e) {
            return -1L;
        }
    }

    private int readActiveWorkerCount() {
        try {
            return bootstrap.getActiveWorkers(Empty.getDefaultInstance()).getWorkersCount();
        } catch (StatusRuntimeException e) {
            return 0;
        }
    }

    private boolean isPriorityTableComplete() {
        try {
            return bootstrap.getPriorityTable(Empty.getDefaultInstance()).getPrioritiesCount()
                    >= ClusterConfig.WORKER_COUNT;
        } catch (StatusRuntimeException e) {
            return false;
        }
    }

    // ------------------------------------------------------------------ election

    public String initiateElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return "Election already in progress - ignoring new election request";
        }
        try {
            int existing = currentCoordinatorId.get();
            if (existing != NO_COORDINATOR) {
                return "Coordinator already present: worker " + existing;
            }
            syncNeighbours();
            CandidateInfo self = candidateSelf();
            String electionId = UUID.randomUUID().toString();
            processedElectionIds.add(electionId);
            System.out.println("[Worker " + workerId + "] election " + electionId + " started, JAC="
                    + self.getJac() + ", neighbours=" + neighbours.size());

            ElectionMessage message = ElectionMessage.newBuilder()
                    .setElectionId(electionId)
                    .setOriginator(self)
                    .setSender(self)
                    .build();
            ProcessedParticipants collected = propagateToNeighbours(message);

            Set<CandidateInfo> allParticipants = new LinkedHashSet<>();
            for (CandidateInfo c : collected.getParticipantsList()) {
                allParticipants.add(c);
            }
            allParticipants.add(self);

            CandidateInfo winner = selectWinner(allParticipants);
            recordCoordinator(winner.getWorkerId());
            System.out.println("[Worker " + workerId + "] election " + electionId + " winner: worker "
                    + winner.getWorkerId() + " (JAC=" + winner.getJac() + ") across "
                    + allParticipants.size() + " reachable workers");

            WinnerAnnouncement announcement = WinnerAnnouncement.newBuilder()
                    .setElectionId(electionId)
                    .setWinner(winner)
                    .addAllParticipants(allParticipants)
                    .build();
            broadcastWinner(announcement);

            return "Coordinator elected: worker " + winner.getWorkerId() + " (JAC=" + winner.getJac()
                    + ") across " + allParticipants.size() + " reachable workers";
        } finally {
            electionInProgress.set(false);
        }
    }

    public ProcessedParticipants receiveElection(ElectionMessage message) {
        if (message == null || message.getElectionId().isBlank()) {
            return null;
        }
        if (!processedElectionIds.add(message.getElectionId())) {
            System.out.println("[Worker " + workerId + "] dropping duplicate election message "
                    + message.getElectionId());
            return null;
        }
        boolean wasInProgress = electionInProgress.getAndSet(true);
        try {
            syncNeighbours();
            CandidateInfo self = candidateSelf();
            ProcessedParticipants.Builder reply = ProcessedParticipants.newBuilder();
            for (CandidateInfo c : message.getParticipantsList()) {
                reply.addParticipants(c);
            }
            reply.addParticipants(self);
            reply.setElectionId(message.getElectionId());
            reply.setSender(self);

            ElectionMessage forwarded = ElectionMessage.newBuilder()
                    .setElectionId(message.getElectionId())
                    .setOriginator(message.getOriginator())
                    .setSender(self)
                    .addAllParticipants(reply.getParticipantsList())
                    .build();
            return propagateToNeighbours(forwarded);
        } finally {
            electionInProgress.set(wasInProgress);
        }
    }

    private ProcessedParticipants propagateToNeighbours(ElectionMessage message) {
        List<CandidateInfo> participants = new ArrayList<>(message.getParticipantsList());
        for (WorkerInfo neighbour : neighbours) {
            if (message.getSender() != null && message.getSender().getWorkerId() == neighbour.getWorkerId()) {
                continue;
            }
            ClusterServiceGrpc.ClusterServiceBlockingStub stub = GrpcChannels.cluster(
                    neighbour.getHost(), neighbour.getPort());
            try {
                // A hard deadline bounds each hop so a dead worker does not stall
                // the whole election (fault tolerance).
                ProcessedParticipants reply = stub.withDeadline(
                                Deadline.after(ClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS,
                                        TimeUnit.MILLISECONDS))
                        .receiveElection(message);
                if (reply != null && reply.getParticipantsCount() > 0) {
                    for (CandidateInfo c : reply.getParticipantsList()) {
                        if (!participants.contains(c)) {
                            participants.add(c);
                        }
                    }
                }
            } catch (StatusRuntimeException e) {
                System.err.println("[Worker " + workerId + "] election propagation to worker "
                        + neighbour.getWorkerId() + " failed: " + e.getStatus().getCode());
            }
        }
        return ProcessedParticipants.newBuilder()
                .setElectionId(message.getElectionId())
                .setSender(message.getSender())
                .addAllParticipants(participants)
                .build();
    }

    private CandidateInfo selectWinner(Collection<CandidateInfo> candidates) {
        return candidates.stream()
                .min(Comparator.comparingInt(CandidateInfo::getJac)
                        .thenComparing(Comparator.comparingInt(CandidateInfo::getPriorityId).reversed())
                        .thenComparing(Comparator.comparingInt(CandidateInfo::getWorkerId).reversed()))
                .orElseThrow(() -> new IllegalStateException("election produced no candidates"));
    }

    private void broadcastWinner(WinnerAnnouncement announcement) {
        for (CandidateInfo candidate : announcement.getParticipantsList()) {
            if (candidate.getWorkerId() == workerId) {
                continue;
            }
            ClusterServiceGrpc.ClusterServiceBlockingStub stub = GrpcChannels.cluster(
                    candidate.getHost(), candidate.getPort());
            try {
                stub.announceWinner(announcement);
            } catch (StatusRuntimeException e) {
                System.err.println("[Worker " + workerId + "] could not announce winner to worker "
                        + candidate.getWorkerId() + ": " + e.getStatus().getCode());
            }
        }
    }

    public void announceWinner(WinnerAnnouncement announcement) {
        if (announcement == null || !announcement.hasWinner()) {
            return;
        }
        processedElectionIds.add(announcement.getElectionId());
        recordCoordinator(announcement.getWinner().getWorkerId());
        System.out.println("[Worker " + workerId + "] recorded coordinator worker "
                + announcement.getWinner().getWorkerId() + " (JAC=" + announcement.getWinner().getJac()
                + ") for election " + announcement.getElectionId());
    }

    public void notifyTermEnd(TermEndNotification notification) {
        System.out.println("[Worker " + workerId + "] received Term_End from coordinator worker "
                + notification.getPriorCoordinatorId() + " -> final JAC table: "
                + notification.getFinalJacTableMap());
    }

    private CandidateInfo candidateSelf() {
        return CandidateInfo.newBuilder()
                .setWorkerId(workerId)
                .setJac(jobAllocationCounter.get())
                .setPriorityId(priorityId)
                .setHost(registeredHost)
                .setPort(registeredPort)
                .build();
    }

    // ------------------------------------------------------------- local segment compute

    public int computePartialMax(List<Integer> numbers) {
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("numbers must not be null or empty");
        }
        return runPartialJob(() -> {
            int partialMax = Collections.max(numbers);
            System.out.println("[Worker " + workerId + "] MAX partial compute -> section="
                    + numbers + ", partialMax=" + partialMax);
            return partialMax;
        });
    }

    public int countPrimes(List<Integer> numbers) {
        if (numbers == null) {
            throw new IllegalArgumentException("numbers must not be null");
        }
        return runPartialJob(() -> {
            int count = 0;
            for (Integer value : numbers) {
                if (isPrime(value)) {
                    count++;
                }
            }
            System.out.println("[Worker " + workerId + "] PRIMECOUNT partial compute -> section="
                    + numbers + ", primes=" + count);
            return count;
        });
    }

    public long sumPrimeRange(int start, int end) {
        if (start < 1 || end < start) {
            throw new IllegalArgumentException("invalid range: start=" + start + ", end=" + end);
        }
        return runPartialJob(() -> {
            long sum = 0;
            for (int n = start; n <= end; n++) {
                if (isPrime(n)) {
                    sum += n;
                }
            }
            System.out.println("[Worker " + workerId + "] PRIMESUM partial compute -> range=["
                    + start + ", " + end + "], sum=" + sum);
            return sum;
        });
    }

    private <T> T runPartialJob(Callable<T> job) {
        try {
            return jobExecutor.submit(job).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new StatusRuntimeException(Status.INTERNAL.withDescription("Segment compute failed")
                    .withCause(cause));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StatusRuntimeException(Status.CANCELLED.withDescription("Segment compute interrupted"));
        }
    }

    private static boolean isPrime(int number) {
        if (number < 2) {
            return false;
        }
        if (number == 2) {
            return true;
        }
        if (number % 2 == 0) {
            return false;
        }
        for (int divisor = 3; divisor <= number / divisor; divisor += 2) {
            if (number % divisor == 0) {
                return false;
            }
        }
        return true;
    }

    // ---------------------------------------------------------------- coordination

    /**
     * ClientService.SubmitJob (server streaming). This may only run on the
     * elected coordinator. Progress is streamed through {@code observer}; the
     * final {@link TaskUpdate} (status TASK_DONE) carries the merged result.
     */
    public void submitJob(com.cs324.proto.JobRequest request,
                          StreamObserver<TaskUpdate> observer) {
        if (currentCoordinatorId.get() != workerId) {
            observer.onError(Status.FAILED_PRECONDITION
                    .withDescription("Worker " + workerId + " is not the coordinator. Current coordinator is "
                            + currentCoordinatorId.get())
                    .asException());
            return;
        }
        final int termSlot;
        try {
            termSlot = claimTermSlot();
        } catch (Exception e) {
            observer.onError(Status.RESOURCE_EXHAUSTED.withDescription(e.getMessage()).asException());
            return;
        }
        System.out.println("[Worker " + workerId + "] submitted job #" + termSlot
                + " -> queued on executor");
        jobExecutor.execute(() -> executeCoordinatorJob(request, observer, termSlot));
    }

    private void executeCoordinatorJob(com.cs324.proto.JobRequest request,
                                       StreamObserver<TaskUpdate> observer,
                                       int termSlot) {
        try {
            String taskId = request.getResumeTaskId().isBlank()
                    ? UUID.randomUUID().toString() : request.getResumeTaskId();
            com.cs324.proto.JobType jobType = request.getJobType();
            System.out.println("[Worker " + workerId + "] executing submitted job #" + termSlot
                    + " on thread " + Thread.currentThread().getName()
                    + " (task " + shortId(taskId) + ", type " + jobType + ")");

            List<Segment> segments;
            if (!request.getResumeTaskId().isBlank()) {
                // Coordinator failover: reconstruct from the replicated ledger instead
                // of restarting from scratch. Completed segments keep their partials,
                // unfinished ones are reassigned/re-run below.
                TaskLedgerEntry existing = readLedgerEntry(taskId);
                if (existing.getTaskId().isBlank()) {
                    // Unknown to this cluster - treat as a fresh job.
                    System.out.println("[Worker " + workerId + "] resume requested for unknown task "
                            + shortId(taskId) + " - starting fresh");
                    segments = buildSegments(request, jobType);
                } else if (existing.getStatus() == TaskStatus.TASK_DONE) {
                    emitCompletedFromLedger(taskId, existing, observer);
                    return;
                } else {
                    segments = rebuildSegmentsFromLedger(existing, request);
                    long doneCount = segments.stream().filter(s -> s.completed).count();
                    System.out.println("[Worker " + workerId + "] resumed task " + shortId(taskId)
                            + " from ledger (" + doneCount + "/" + segments.size() + " segments already done)");
                }
            } else {
                segments = buildSegments(request, jobType);
            }
            TaskLedgerEntry ledger = publishTaskStart(request, taskId, jobType, segments);
            emitRunning(observer, taskId, request.getClientId(), jobType, segments, 0);

            TaskUpdate finalUpdate = runSegmentsWithFaultTolerance(
                    request, taskId, jobType, segments, observer, ledger);
            observer.onNext(finalUpdate);
            observer.onCompleted();
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] job failed: " + e.getStatus().getDescription());
            observer.onError(e);
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] job failed: " + e.getMessage());
            observer.onError(Status.INTERNAL.withDescription(String.valueOf(e.getMessage())).asException());
        } finally {
            maybeEndCoordinatorTerm();
        }
    }

    /**
     * Runs every segment of a task, defending against worker failure: each
     * segment call has a deadline; on failure the segment is re-assigned to a
     * fresh reachable worker and retried (up to {@link
     * ClusterConfig#MAX_SEGMENT_RETRIES}). The task ledger is updated
     * after every segment and a progress update is streamed after each success.
     */
    private TaskUpdate runSegmentsWithFaultTolerance(com.cs324.proto.JobRequest request,
                                                     String taskId,
                                                     com.cs324.proto.JobType jobType,
                                                     List<Segment> segments,
                                                     StreamObserver<TaskUpdate> observer,
                                                     TaskLedgerEntry ledger) {
        long aggregate = jobType == com.cs324.proto.JobType.MAX ? Long.MIN_VALUE : 0L;
        int completedCount = 0;
        List<Integer> usedWorkers = new ArrayList<>();
        Map<Integer, WorkerNode.Accumulator> progress = new HashMap<>();

        for (Segment segment : segments) {
            if (segment.completed) {
                aggregate = mergePartial(jobType, aggregate, segment.partial);
                completedCount++;
                usedWorkers.add(segment.workerId);
                progress.computeIfAbsent(segment.workerId, k -> new WorkerNode.Accumulator())
                        .completed++;
                continue;
            }

            int attempts = 0;
            boolean done = false;
            while (!done && attempts <= ClusterConfig.MAX_SEGMENT_RETRIES) {
                int targetId = segment.workerId;
                WorkerInfo target = lookupRegisteredWorker(targetId);
                if (target == null || !online(targetId)) {
                    // Worker offline before the call -> reassign immediately.
                    System.err.println("[Worker " + workerId + "] segment " + segment.index
                            + " target worker " + targetId + " offline - reassigning");
                    targetId = reassignSegment(segment, usedWorkers, progress);
                    if (targetId < 1) {
                        pushFailureToLedger(taskId, segment);
                        throw new StatusRuntimeException(Status.UNAVAILABLE
                                .withDescription("No reachable worker available for task " + shortId(taskId)));
                    }
                    segment.workerId = targetId;
                    target = lookupRegisteredWorker(targetId);
                    if (target == null) {
                        attempts++;
                        continue;
                    }
                }
                attempts++;
                try {
                    ClusterServiceGrpc.ClusterServiceBlockingStub stub =
                            GrpcChannels.cluster(target.getHost(), target.getPort())
                                    .withDeadlineAfter(ClusterConfig.SEGMENT_CALL_TIMEOUT_MS,
                                            TimeUnit.MILLISECONDS);
                    long partial = callSegment(stub, jobType, segment);
                    // JAC tracking: the worker that processed this segment has its
                    // own local JAC incremented by the coordinator.
                    try {
                        GrpcChannels.cluster(target.getHost(), target.getPort())
                                .recordAllocation(Empty.getDefaultInstance());
                    } catch (StatusRuntimeException ignored) {
                        // best effort bookkeeping
                    }
                    segment.completed = true;
                    segment.partial = partial;
                    aggregate = mergePartial(jobType, aggregate, partial);
                    completedCount++;
                    usedWorkers.add(targetId);
                    progress.computeIfAbsent(targetId, k -> new WorkerNode.Accumulator())
                            .completed++;

                    pushSegmentDoneToLedger(taskId, segment);
                    emitProgress(observer, taskId, request.getClientId(), jobType, segments,
                            completedCount, usedWorkers, progress, aggregate);
                    System.out.println("[Worker " + workerId + "] " + jobType + " task " + shortId(taskId)
                            + " segment " + (segment.index + 1) + "/" + segments.size()
                            + " <- worker " + targetId + ": " + partial + " (attempt " + attempts + ")");
                    done = true;
                } catch (StatusRuntimeException e) {
                    System.err.println("[Worker " + workerId + "] segment " + segment.index
                            + " on worker " + targetId + " failed (" + e.getStatus().getCode()
                            + ") - reassigning");
                    markWorkerFailed(targetId, progress);
                    pushSegmentFailedToLedger(taskId, segment);
                    int replacement = reassignSegment(segment, usedWorkers, progress);
                    if (replacement < 1) {
                        pushFailureToLedger(taskId, segment);
                        throw new StatusRuntimeException(Status.UNAVAILABLE
                                .withDescription("No reachable worker available for task " + shortId(taskId)));
                    }
                    segment.workerId = replacement;
                }
            }
        }

        // Compile the final result and completion state.
        long finalResult = aggregate == Long.MIN_VALUE ? 0L : aggregate;
        pushTaskCompletion(taskId, segments, jobType, finalResult, usedWorkers);

        TaskUpdate.Builder finalUpdate = TaskUpdate.newBuilder()
                .setTaskId(taskId)
                .setClientId(request.getClientId())
                .setJobType(jobType)
                .setStatus(TaskStatus.TASK_DONE)
                .setPercent(100)
                .setSegmentsTotal(segments.size())
                .setSegmentsCompleted(segments.size())
                .setResult(finalResult)
                .setCoordinatorId(workerId)
                .setMessage("done");
        for (int used : usedWorkers) {
            finalUpdate.addUsedWorkerIds(used);
        }
        addWorkerProgress(finalUpdate, progress, segments);
        recordTask(finalUpdate.build());
        System.out.println("[Worker " + workerId + "] " + jobType + " task " + shortId(taskId)
                + " complete -> result=" + finalResult + ", workers=" + usedWorkers);
        return finalUpdate.build();
    }

    private static long mergePartial(com.cs324.proto.JobType jobType, long aggregate, long partial) {
        return jobType == com.cs324.proto.JobType.MAX ? Math.max(aggregate, partial) : aggregate + partial;
    }

    private long callSegment(ClusterServiceGrpc.ClusterServiceBlockingStub stub,
                             com.cs324.proto.JobType jobType, Segment segment) {
        return switch (jobType) {
            case MAX -> stub.computePartialMax(
                    com.cs324.proto.NumbersRequest.newBuilder().addAllNumbers(segment.chunk).build()).getValue();
            case PRIMECOUNT -> stub.countPrimes(
                    com.cs324.proto.NumbersRequest.newBuilder().addAllNumbers(segment.chunk).build()).getValue();
            case PRIMESUM -> stub.sumPrimeRange(
                    com.cs324.proto.RangeRequest.newBuilder().setStart(segment.rangeStart)
                            .setEnd(segment.rangeEnd).build()).getValue();
            default -> throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                    .withDescription("unsupported job type: " + jobType));
        };
    }

    private int reassignSegment(Segment segment, List<Integer> usedWorkers,
                                Map<Integer, WorkerNode.Accumulator> progress) {
        List<WorkerInfo> assignees = getAssignmentWorkers();
        // Skip workers already used in this task and workers we just failed on.
        for (WorkerInfo candidate : assignees) {
            WorkerNode.Accumulator acc = progress.get(candidate.getWorkerId());
            boolean usedThisTask = usedWorkers.contains(candidate.getWorkerId());
            boolean failedThisTask = acc != null && acc.failed;
            if (!usedThisTask && !failedThisTask) {
                return candidate.getWorkerId();
            }
        }
        // Fall back to any worker that is online (may already hold a segment).
        for (WorkerInfo candidate : assignees) {
            WorkerNode.Accumulator acc = progress.get(candidate.getWorkerId());
            if (acc == null || !acc.failed) {
                return candidate.getWorkerId();
            }
        }
        return -1;
    }

    private void markWorkerFailed(int workerId, Map<Integer, WorkerNode.Accumulator> progress) {
        progress.computeIfAbsent(workerId, k -> new WorkerNode.Accumulator()).failed = true;
    }

    // ------------------------------------------------------------------ progress

    private void emitRunning(StreamObserver<TaskUpdate> observer, String taskId, String clientId,
                             com.cs324.proto.JobType jobType, List<Segment> segments, int completed) {
        emitProgress(observer, taskId, clientId, jobType, segments, completed,
                new ArrayList<>(), new HashMap<>(), jobType == com.cs324.proto.JobType.MAX ? Long.MIN_VALUE : 0L);
    }

    private void emitProgress(StreamObserver<TaskUpdate> observer, String taskId, String clientId,
                              com.cs324.proto.JobType jobType, List<Segment> segments, int completed,
                              List<Integer> usedWorkers, Map<Integer, WorkerNode.Accumulator> progress,
                              long aggregate) {
        int percent = segments.isEmpty() ? 0 : Math.min(100, completed * 100 / segments.size());
        TaskUpdate.Builder builder = TaskUpdate.newBuilder()
                .setTaskId(taskId)
                .setClientId(clientId)
                .setJobType(jobType)
                .setStatus(TaskStatus.TASK_RUNNING)
                .setPercent(percent)
                .setSegmentsTotal(segments.size())
                .setSegmentsCompleted(completed)
                .setCoordinatorId(workerId)
                .setMessage((completed == 0 ? "started" : "segment " + completed + "/" + segments.size()));
        if (aggregate != Long.MIN_VALUE) {
            builder.setResult(aggregate);
        }
        addWorkerProgress(builder, progress, segments);
        synchronized (observer) {
            try {
                observer.onNext(builder.build());
            } catch (RuntimeException ignored) {
                // the client went away mid-stream; keep executing for the ledger
            }
        }
    }

    private void addWorkerProgress(TaskUpdate.Builder builder,
                                   Map<Integer, WorkerNode.Accumulator> progress,
                                   List<Segment> segments) {
        Map<Integer, Integer> assigned = new HashMap<>();
        for (Segment s : segments) {
            assigned.merge(s.workerId, 1, Integer::sum);
        }
        Set<Integer> ids = new HashSet<>(assigned.keySet());
        ids.addAll(progress.keySet());
        List<Integer> sorted = new ArrayList<>(ids);
        sorted.sort(Integer::compareTo);
        for (int id : sorted) {
            WorkerNode.Accumulator acc = progress.getOrDefault(id, new WorkerNode.Accumulator());
            WorkerProgress wp = WorkerProgress.newBuilder()
                    .setWorkerId(id)
                    .setTasksAssigned(assigned.getOrDefault(id, 0))
                    .setTasksCompleted(acc.completed)
                    .setJac(safeReadJac(id))
                    .setOnline(online(id))
                    .setStatus(acc.failed ? "FAILED" : (acc.completed > 0 ? "RUNNING" : "IDLE"))
                    .build();
            builder.addWorkers(wp);
        }
    }

    private void emitCompletedFromLedger(String taskId, TaskLedgerEntry entry,
                                         StreamObserver<TaskUpdate> observer) {
        List<Integer> used = new ArrayList<>(entry.getUsedWorkersList());
        TaskUpdate update = TaskUpdate.newBuilder()
                .setTaskId(taskId)
                .setClientId(entry.getClientId())
                .setJobType(entry.getJobType())
                .setStatus(TaskStatus.TASK_DONE)
                .setPercent(100)
                .setSegmentsTotal(entry.getSegmentsTotal())
                .setSegmentsCompleted(entry.getSegmentsTotal())
                .setResult(entry.getFinalResult())
                .setCoordinatorId(entry.getCoordinatorId())
                .addAllUsedWorkerIds(used)
                .setMessage("recovered from ledger")
                .build();
        observer.onNext(update);
        observer.onCompleted();
    }

    // ------------------------------------------------------------------ task ledger

    private TaskLedgerEntry publishTaskStart(com.cs324.proto.JobRequest request, String taskId,
                                             com.cs324.proto.JobType jobType, List<Segment> segments) {
        TaskLedgerEntry.Builder entry = TaskLedgerEntry.newBuilder()
                .setTaskId(taskId)
                .setClientId(request.getClientId())
                .setJobType(jobType)
                .setCoordinatorId(workerId)
                .setStartedAtMs(System.currentTimeMillis())
                .setSegmentsTotal(segments.size())
                .setStatus(TaskStatus.TASK_RUNNING);
        for (Segment s : segments) {
            com.cs324.proto.SegmentRecord.Builder rec = com.cs324.proto.SegmentRecord.newBuilder()
                    .setSegmentIndex(s.index)
                    .setWorkerId(s.workerId)
                    .setCompleted(s.completed)
                    .setPartial(s.partial)
                    .setFailed(!s.completed && s.failed);
            if (s.rangeStart >= 0) {
                rec.setRangeStart(s.rangeStart).setRangeEnd(s.rangeEnd);
            } else if (s.chunk != null) {
                rec.setDataStart(s.dataStart).setDataLength(s.chunk.size());
            }
            entry.putSegments(s.index, rec.build());
        }
        TaskLedgerEntry built = entry.build();
        try {
            bootstrap.recordTaskStart(built);
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] could not publish task start for "
                    + shortId(taskId) + ": " + e.getStatus().getCode());
        }
        return built;
    }

    private void pushSegmentDoneToLedger(String taskId, Segment segment) {
        try {
            bootstrap.recordSegmentUpdate(SegmentUpdate.newBuilder()
                    .setTaskId(taskId)
                    .setSegmentIndex(segment.index)
                    .setWorkerId(segment.workerId)
                    .setCompleted(true)
                    .setPartial(segment.partial)
                    .build());
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] ledger segment update failed: " + e.getStatus().getCode());
        }
    }

    private void pushSegmentFailedToLedger(String taskId, Segment segment) {
        try {
            bootstrap.recordSegmentUpdate(SegmentUpdate.newBuilder()
                    .setTaskId(taskId)
                    .setSegmentIndex(segment.index)
                    .setWorkerId(segment.workerId)
                    .setFailed(true)
                    .build());
        } catch (StatusRuntimeException ignored) {
        }
    }

    private void pushFailureToLedger(String taskId, Segment segment) {
        try {
            bootstrap.recordSegmentUpdate(SegmentUpdate.newBuilder()
                    .setTaskId(taskId)
                    .setSegmentIndex(segment.index)
                    .setWorkerId(segment.workerId)
                    .setFailed(true)
                    .build());
        } catch (StatusRuntimeException ignored) {
        }
    }

    private void pushTaskCompletion(String taskId, List<Segment> segments, com.cs324.proto.JobType jobType,
                                    long finalResult, List<Integer> usedWorkers) {
        long startedAt = readLedgerEntry(taskId).getStartedAtMs();
        TaskLedgerEntry.Builder entry = TaskLedgerEntry.newBuilder()
                .setTaskId(taskId)
                .setClientId(readLedgerEntry(taskId).getClientId())
                .setJobType(jobType)
                .setCoordinatorId(workerId)
                .setStartedAtMs(startedAt)
                .setSegmentsTotal(segments.size())
                .setFinalResult(finalResult)
                .setStatus(TaskStatus.TASK_DONE)
                .setAccumulatedPartial(finalResult == Long.MIN_VALUE ? 0 : finalResult);
        for (Integer used : usedWorkers) {
            entry.addUsedWorkers(used);
        }
        for (Segment s : segments) {
            entry.putSegments(s.index, com.cs324.proto.SegmentRecord.newBuilder()
                    .setSegmentIndex(s.index)
                    .setWorkerId(s.workerId)
                    .setCompleted(s.completed)
                    .setPartial(s.partial)
                    .setFailed(s.failed)
                    .build());
        }
        try {
            bootstrap.recordTaskCompletion(entry.build());
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] ledger completion update failed: "
                    + e.getStatus().getCode());
        }
    }

    /**
     * Rebuilds the exact segment list of a previously-published task from the
     * replicated ledger, so a fresh coordinator can continue (not restart) the
     * task: completed segments keep their partials, unfinished ones are re-run.
     */
    private List<Segment> rebuildSegmentsFromLedger(TaskLedgerEntry entry, com.cs324.proto.JobRequest request) {
        List<Segment> segments = new ArrayList<>();
        List<Integer> numbers = request.getNumbersList();
        for (int i = 0; i < entry.getSegmentsTotal(); i++) {
            com.cs324.proto.SegmentRecord rec = entry.getSegmentsMap().get(i);
            if (rec == null) continue;
            List<Integer> chunk = null;
            int rangeStart = -1, rangeEnd = -1;
            if (entry.getJobType() == com.cs324.proto.JobType.PRIMESUM) {
                rangeStart = rec.getRangeStart();
                rangeEnd = rec.getRangeEnd();
            } else if (rec.getDataLength() > 0 && !numbers.isEmpty()) {
                int from = Math.min(rec.getDataStart(), numbers.size());
                int to = Math.min(rec.getDataStart() + rec.getDataLength(), numbers.size());
                chunk = new ArrayList<>(numbers.subList(from, to));
            }
            segments.add(new Segment(rec.getSegmentIndex(), chunk, rec.getDataStart(),
                    rangeStart, rangeEnd, rec.getWorkerId(),
                    rec.getCompleted(), rec.getPartial(), rec.getFailed()));
        }
        return segments;
    }

    private TaskLedgerEntry readLedgerEntry(String taskId) {
        try {
            return bootstrap.getTaskLedger(com.cs324.proto.TaskQuery.newBuilder().setTaskId(taskId).build());
        } catch (StatusRuntimeException e) {
            return TaskLedgerEntry.getDefaultInstance();
        }
    }

    /** Rebuilds task state from the ledger for a client that reconnects after
     *  coordinator failover. */
    public TaskUpdate getTaskStatus(String taskId) {
        TaskLedgerEntry entry = readLedgerEntry(taskId);
        if (entry.getTaskId().isBlank()) {
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("unknown task " + taskId));
        }
        return TaskUpdate.newBuilder()
                .setTaskId(entry.getTaskId())
                .setClientId(entry.getClientId())
                .setJobType(entry.getJobType())
                .setStatus(entry.getStatus())
                .setPercent(entry.getStatus() == TaskStatus.TASK_DONE ? 100 : 0)
                .setSegmentsTotal(entry.getSegmentsTotal())
                .setSegmentsCompleted(entry.getStatus() == TaskStatus.TASK_DONE ? entry.getSegmentsTotal() : 0)
                .setResult(entry.getFinalResult())
                .setCoordinatorId(entry.getCoordinatorId())
                .build();
    }

    public List<TaskUpdate> getTaskHistory() {
        return new ArrayList<>(taskHistory);
    }

    private void recordTask(TaskUpdate record) {
        taskHistory.addFirst(record);
        while (taskHistory.size() > MAX_TASK_HISTORY) {
            taskHistory.pollLast();
        }
    }

    // ------------------------------------------------------------ segmentation

    private List<Segment> buildSegments(com.cs324.proto.JobRequest request,
                                        com.cs324.proto.JobType jobType) {
        List<WorkerInfo> assignees = getAssignmentWorkers();
        if (assignees.isEmpty()) {
            throw new StatusRuntimeException(Status.UNAVAILABLE
                    .withDescription("No reachable workers are available for " + jobType + " job"));
        }
        List<Segment> segments = new ArrayList<>();
        if (jobType == com.cs324.proto.JobType.PRIMESUM) {
            int start = request.getRangeStart();
            int end = request.getRangeEnd();
            int items = end - start + 1;
            int segmentCount = segmentCountFor(assignees.size(), items, SEGMENT_RANGE_LENGTH);
            int segmentStart = start;
            for (int i = 0; i < segmentCount; i++) {
                int remaining = end - segmentStart + 1;
                int remainingSegments = segmentCount - i;
                int chunkSize = (remaining + remainingSegments - 1) / remainingSegments;
                int segmentEnd = segmentStart + chunkSize - 1;
                segments.add(new Segment(i, null, -1, segmentStart, segmentEnd,
                        assignees.get(i).getWorkerId(), false, 0, false));
                segmentStart = segmentEnd + 1;
            }
        } else {
            List<Integer> numbers = request.getNumbersList();
            if (numbers.isEmpty()) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT
                        .withDescription("numbers must not be empty for " + jobType + " job"));
            }
            int segmentCount = segmentCountFor(assignees.size(), numbers.size(), SEGMENT_NUMBERS);
            int start = 0;
            for (int i = 0; i < segmentCount; i++) {
                int remaining = numbers.size() - start;
                int remainingSegments = segmentCount - i;
                int chunkSize = (remaining + remainingSegments - 1) / remainingSegments;
                List<Integer> chunk = new ArrayList<>(numbers.subList(start, start + chunkSize));
                segments.add(new Segment(i, chunk, start, -1, -1,
                        assignees.get(i).getWorkerId(), false, 0, false));
                start += chunkSize;
            }
        }
        return segments;
    }

    private static int segmentCountFor(int activeWorkers, int totalItems, int perSegment) {
        if (totalItems <= 0) {
            return 0;
        }
        return Math.max(1, Math.min(activeWorkers, (totalItems + perSegment - 1) / perSegment));
    }

    // ------------------------------------------------------------ reachability

    /**
     * Every reachable, online worker (BFS over the neighbour ring), sorted so the
     * least-loaded workers come first (JAC ascending, then startup priority
     * descending, then worker id ascending). Fetches each worker's live state, so
     * a crashed worker drops out of the assignment pool automatically.
     */
    private List<WorkerInfo> getAssignmentWorkers() {
        Map<Integer, WorkerInfo> active = new HashMap<>();
        for (WorkerInfo w : safeOnlineWorkers()) {
            active.put(w.getWorkerId(), w);
        }
        active.put(workerId, selfInfo());

        List<Integer> reachableIds = new ArrayList<>();
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> pending = new ArrayDeque<>();
        pending.add(workerId);
        visited.add(workerId);

        while (!pending.isEmpty()) {
            int nextId = pending.remove();
            if (nextId != workerId) {
                reachableIds.add(nextId);
            }
            List<WorkerInfo> nbrs = safeNeighboursOf(nextId, active);
            for (WorkerInfo n : nbrs) {
                if (active.containsKey(n.getWorkerId()) && visited.add(n.getWorkerId())) {
                    pending.add(n.getWorkerId());
                }
            }
        }

        Map<Integer, WorkerState> states = new HashMap<>();
        for (int id : reachableIds) {
            WorkerState state = readWorkerState(id);
            if (state != null) {
                states.put(id, state);
            }
        }
        List<WorkerInfo> candidates = new ArrayList<>();
        for (int id : reachableIds) {
            if (id == workerId) {
                continue;
            }
            WorkerState state = states.get(id);
            WorkerInfo info = active.get(id);
            if (state == null || info == null) {
                continue;
            }
            if (lastSeen(id) >= ClusterConfig.HEARTBEAT_TIMEOUT_MS) {
                continue; // bootstrap says the heartbeat is stale
            }
            candidates.add(info);
        }
        candidates.sort(Comparator
                .comparingInt((WorkerInfo w) -> states.get(w.getWorkerId()).getJac())
                .thenComparing(Comparator.comparingInt(
                        (WorkerInfo w) -> states.get(w.getWorkerId()).getPriorityId()).reversed())
                .thenComparingInt(WorkerInfo::getWorkerId));
        return candidates;
    }

    private List<WorkerInfo> safeNeighboursOf(int targetId, Map<Integer, WorkerInfo> active) {
        if (targetId == workerId) {
            return new ArrayList<>(neighbours);
        }
        WorkerInfo worker = active.get(targetId);
        if (worker == null) {
            return Collections.emptyList();
        }
        try {
            return GrpcChannels.cluster(worker.getHost(), worker.getPort())
                    .withDeadline(Deadline.after(ClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS,
                            TimeUnit.MILLISECONDS))
                    .getNeighbours(Empty.getDefaultInstance()).getNeighboursList();
        } catch (StatusRuntimeException e) {
            return Collections.emptyList();
        }
    }

    private WorkerState readWorkerState(int targetId) {
        if (targetId == workerId) {
            return getWorkerState();
        }
        WorkerInfo worker = lookupRegisteredWorker(targetId);
        if (worker == null) {
            return null;
        }
        try {
            return GrpcChannels.cluster(worker.getHost(), worker.getPort())
                    .withDeadline(Deadline.after(ClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS,
                            TimeUnit.MILLISECONDS))
                    .getWorkerState(Empty.getDefaultInstance());
        } catch (StatusRuntimeException e) {
            return null;
        }
    }

    private int safeReadJac(int targetId) {
        WorkerState state = readWorkerState(targetId);
        if (state == null) {
            return Integer.MAX_VALUE;
        }
        if (state.getWorkerId() == workerId) {
            return jobAllocationCounter.get();
        }
        return state.getJac();
    }

    private boolean online(int targetId) {
        WorkerInfo info = lookupRegisteredWorker(targetId);
        if (info == null) {
            return false;
        }
        try {
            GrpcChannels.cluster(info.getHost(), info.getPort())
                    .withDeadlineAfter(ClusterConfig.COORDINATOR_PROBE_TIMEOUT_MS,
                            TimeUnit.MILLISECONDS)
                    .getWorkerState(Empty.getDefaultInstance());
            return true;
        } catch (StatusRuntimeException e) {
            return false;
        }
    }

    private List<WorkerInfo> safeActiveWorkers() {
        try {
            return bootstrap.getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
        } catch (StatusRuntimeException e) {
            return Collections.emptyList();
        }
    }

    private List<WorkerInfo> safeOnlineWorkers() {
        List<WorkerInfo> all = safeActiveWorkers();
        List<WorkerInfo> onlineWorkers = new ArrayList<>();
        for (WorkerInfo w : all) {
            if (lastSeen(w.getWorkerId()) < ClusterConfig.HEARTBEAT_TIMEOUT_MS) {
                onlineWorkers.add(w);
            }
        }
        return onlineWorkers;
    }

    private long lastSeen(int targetId) {
        try {
            // The bootstrap's heartbeat watchdog decides freshness; a recently
            // registered worker counts as fresh.
            return bootstrapActiveFreshMs(targetId);
        } catch (StatusRuntimeException e) {
            return Long.MAX_VALUE;
        }
    }

    private long bootstrapActiveFreshMs(int targetId) {
        List<WorkerInfo> workers = safeOnlineWorkersAll();
        for (WorkerInfo w : workers) {
            if (w.getWorkerId() == targetId) {
                return w.getOnline() ? 0L : Long.MAX_VALUE;
            }
        }
        return Long.MAX_VALUE;
    }

    private List<WorkerInfo> safeOnlineWorkersAll() {
        try {
            return bootstrap.getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
        } catch (StatusRuntimeException e) {
            return Collections.emptyList();
        }
    }

    private WorkerInfo lookupRegisteredWorker(int targetId) {
        for (WorkerInfo w : safeActiveWorkers()) {
            if (w.getWorkerId() == targetId) {
                return w;
            }
        }
        return targetId == workerId ? selfInfo() : null;
    }

    private void callSetCoordinator(WorkerInfo target, int coordinatorId) {
        try {
            GrpcChannels.cluster(target.getHost(), target.getPort())
                    .setCurrentCoordinator(com.cs324.proto.SetCoordinatorRequest.newBuilder()
                            .setCoordinatorId(coordinatorId).build());
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] could not notify worker " + target.getWorkerId()
                    + " of the initial coordinator: " + e.getStatus().getCode());
        }
    }

    private boolean bootstrapOnline() {
        return true; // this worker is running if it answers
    }

    // ------------------------------------------------------------ term lifecycle

    private int claimTermSlot() {
        for (; ; ) {
            int current = jobsThisTerm.get();
            if (current >= COORDINATOR_TERM_LIMIT) {
                throw new StatusRuntimeException(Status.RESOURCE_EXHAUSTED.withDescription(
                        "Coordinator term ended after " + COORDINATOR_TERM_LIMIT
                                + " jobs; no jobs can be processed until the next leader election completes"));
            }
            if (jobsThisTerm.compareAndSet(current, current + 1)) {
                return current + 1;
            }
        }
    }

    private void maybeEndCoordinatorTerm() {
        if (jobsThisTerm.get() < COORDINATOR_TERM_LIMIT) {
            return;
        }
        if (!currentCoordinatorId.compareAndSet(workerId, NO_COORDINATOR)) {
            return; // another finished job already ended this term
        }
        System.out.println("[Worker " + workerId + "] completed " + COORDINATOR_TERM_LIMIT
                + " jobs this term - ending term and starting a new leader election");
        Map<Integer, Integer> jacTable = broadcastTermEnd();

        int highestNodeJac = Integer.MIN_VALUE;
        for (Map.Entry<Integer, Integer> e : jacTable.entrySet()) {
            if (e.getKey() != workerId) {
                highestNodeJac = Math.max(highestNodeJac, e.getValue());
            }
        }
        if (highestNodeJac != Integer.MIN_VALUE) {
            int demotionTarget = highestNodeJac + 1;
            if (jobAllocationCounter.get() <= highestNodeJac) {
                jobAllocationCounter.addAndGet(demotionTarget - jobAllocationCounter.get());
            }
        }
        System.out.println("[Worker " + workerId + "] demotion tick -> JAC is now "
                + jobAllocationCounter.get());

        try {
            initiateElection();
        } catch (StatusRuntimeException e) {
            System.err.println("[Worker " + workerId + "] term re-election failed: " + e.getStatus().getCode());
        }
    }

    private Map<Integer, Integer> broadcastTermEnd() {
        TermEndNotification.Builder notification = TermEndNotification.newBuilder()
                .setPriorCoordinatorId(workerId);
        Map<Integer, Integer> jacTable = new HashMap<>();
        try {
            for (WorkerInfo w : safeOnlineWorkers()) {
                int targetId = w.getWorkerId();
                jacTable.put(targetId, safeReadJac(targetId));
            }
            notification.putAllFinalJacTable(jacTable);
            System.out.println("[Worker " + workerId + "] Term_End broadcast -> final JAC table: " + jacTable);
            for (WorkerInfo w : safeOnlineWorkers()) {
                if (w.getWorkerId() == workerId) {
                    continue;
                }
                try {
                    GrpcChannels.cluster(w.getHost(), w.getPort()).notifyTermEnd(notification.build());
                } catch (StatusRuntimeException e) {
                    System.err.println("[Worker " + workerId + "] could not deliver Term_End to worker "
                            + w.getWorkerId() + ": " + e.getStatus().getCode());
                }
            }
        } catch (Exception e) {
            System.err.println("[Worker " + workerId + "] Term_End broadcast failed: " + e.getMessage());
            return Collections.emptyMap();
        }
        jacTable.put(workerId, jobAllocationCounter.get());
        return jacTable;
    }

    // ---------------------------------------------------------------- segment model

    private static final class Segment {
        final int index;
        final List<Integer> chunk;    // MAX / PRIMECOUNT payload (null for PRIMESUM)
        final int dataStart;          // MAX / PRIMECOUNT: begin index into the task numbers
        final int rangeStart;         // PRIMESUM only
        final int rangeEnd;           // PRIMESUM only
        int workerId;
        boolean completed;
        long partial;
        boolean failed;

        Segment(int index, List<Integer> chunk, int dataStart, int rangeStart, int rangeEnd,
                int workerId, boolean completed, long partial, boolean failed) {
            this.index = index;
            this.chunk = chunk;
            this.dataStart = dataStart;
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.workerId = workerId;
            this.completed = completed;
            this.partial = partial;
            this.failed = failed;
        }
    }

    private static String shortId(String taskId) {
        return taskId == null ? "null" : (taskId.length() > 8 ? taskId.substring(0, 8) : taskId);
    }

    /** Mutable per-worker progress accumulator used while a task runs. */
    public static final class Accumulator {
        public int assigned;
        public int completed;
        public boolean failed;
    }
}