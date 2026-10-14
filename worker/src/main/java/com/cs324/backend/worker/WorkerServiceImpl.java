package com.cs324.backend.worker;

import com.cs324.common.ChannelPool;
import com.cs324.common.WorkerClusterConfig;
import com.cs324.grpc.BootstrapServiceGrpc;
import com.cs324.grpc.CandidateInfo;
import com.cs324.grpc.ElectionMessage;
import com.cs324.grpc.Empty;
import com.cs324.grpc.IntValue;
import com.cs324.grpc.MaxJobRequest;
import com.cs324.grpc.NumberList;
import com.cs324.grpc.PrimeCountRequest;
import com.cs324.grpc.PrimeSumRequest;
import com.cs324.grpc.ProcessedParticipants;
import com.cs324.grpc.RangeRequest;
import com.cs324.grpc.StringValue;
import com.cs324.grpc.TaskHistory;
import com.cs324.grpc.TaskResult;
import com.cs324.grpc.TermEndRequest;
import com.cs324.grpc.WinnerAnnouncement;
import com.cs324.grpc.WorkerId;
import com.cs324.grpc.WorkerInfo;
import com.cs324.grpc.WorkerList;
import com.cs324.grpc.WorkerServiceGrpc;

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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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
 * Worker Node service. Implements the generated {@code WorkerService} gRPC
 * contract and runs the distributed leader election protocol.
 *
 * <p>Election algorithm (echo/termination aggregation):
 * <ul>
 *   <li>An ELECTION message carries a unique election id and a growing set of
 *       discovered participants. Each worker keeps a thread-safe set of
 *       processed election ids so no message is handled twice and message loops
 *       are cut.</li>
 *   <li>The initiator broadcasts over its neighbour ring. Every reachable,
 *       active worker adds its own {@link CandidateInfo} snapshot and re-sends
 *       the message to its neighbours (except the hop it came from), and echoes
 *       the merged participant set back. When all echoes return, every reachable
 *       worker has been discovered.</li>
 *   <li>The winner is the reachable worker with the lowest JAC, excluding the
 *       previous coordinator after a completed term whenever another candidate
 *       is available; ties use the lowest worker id.</li>
 *   <li>Phase 1: on a fresh cluster the workers register with Bootstrap, which
 *       only maintains the active-worker registry. Workers rebuild neighbours
 *       from that registry and the highest active worker starts the distributed
 *       ELECTION message; the winner is still selected by the JAC/rotation rule.</li>
 *   <li>Phase 2: the coordinator re-splits each incoming task into a variable
 *       number of contiguous segments (never more than one per worker, sized by
 *       the input) and hands them to reachable workers. Each worker that receives
 *       a segment increments its own JAC.</li>
 *   <li>Phase 3: after 5 submitted jobs the coordinator broadcasts a Term_End
 *       message with the verified JAC table, steps down, and a new election picks
 *       the lowest-JAC worker (ties rotate away from the prior coordinator).</li>
 *   <li>The result is propagated through the worker network as a COORDINATOR
 *       {@link WinnerAnnouncement}, so every reachable worker records the same
 *       coordinator.</li>
 *   <li>The Bootstrap Node is never involved beyond looking up the active worker
 *       registry and sequencing lottery claims; it does not participate in the
 *       election.</li>
 * </ul>
 *
 * <p>All remote calls are blocking gRPC calls made through {@link WorkerClient}
 * handles, which wrap either this node itself or a stub to a peer.
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private static final Empty EMPTY = Empty.getDefaultInstance();
    private final int workerId;
    private final WorkerInfo selfInfo;
    private final String registeredHost;
    private final int registeredPort;
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final ChannelPool channelPool = new ChannelPool();
    private final SelfClient selfClient;

    private final AtomicInteger jobAllocationCounter = new AtomicInteger(0);
    private final AtomicInteger currentCoordinatorId = new AtomicInteger(WorkerClusterConfig.NO_COORDINATOR);
    private final AtomicInteger previousCoordinatorId = new AtomicInteger(WorkerClusterConfig.NO_COORDINATOR);
    private final Set<String> processedElectionIds = ConcurrentHashMap.newKeySet();
    private final Set<String> processedCoordinatorAnnouncements = ConcurrentHashMap.newKeySet();
    private final Set<WorkerInfo> neighbours = ConcurrentHashMap.newKeySet();
    private final String leaderman = "cs324";

    /**
     * Number of submitted jobs accepted by this coordinator during the current term.
     * Deliberately separate from the JAC: the JAC counts every job segment this
     * worker receives; this counter reserves up to five client job slots before
     * the next election.
     */
    private final AtomicInteger jobsThisTerm = new AtomicInteger(0);
    private final AtomicInteger completedJobsThisTerm = new AtomicInteger(0);
    private final AtomicBoolean termEndScheduled = new AtomicBoolean(false);
    private final AtomicBoolean electionVacancyAuthorized = new AtomicBoolean(true);

    private final AtomicInteger jobThreadCounter = new AtomicInteger(0);

    /**
     * Recent distributed-task records compiled while this worker acted as the
     * coordinator, newest first. Thread-safe under concurrent client jobs; capped
     * so the JVM does not accumulate them.
     */
    private static final int MAX_TASK_HISTORY = 50;
    private final Deque<TaskResult> taskHistory = new ConcurrentLinkedDeque<>();

    /** Startup-lottery priority id (1..N); N = highest, 0 until minted. */
    private volatile int priorityId = 0;

    /** Wall-clock time this worker's randomized countdown expires (Phase 1). */
    private volatile long lotteryFireAt = -1L;

    /** True once this worker rebuilt its neighbour ring after the lottery completed. */
    private volatile boolean neighboursSyncedAfterLottery = false;

    private final ScheduledExecutorService lotteryScheduler;

    /** Guards against a worker running two elections at once. */
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    /** Runs submitted jobs and partial computations concurrently. */
    private final ExecutorService jobExecutor;

    private final ScheduledExecutorService electionScheduler;

    public WorkerServiceImpl(int workerId, WorkerInfo self, String bootstrapHost, int bootstrapPort) {
        this.workerId = workerId;
        this.selfInfo = self;
        this.registeredHost = self.getHost();
        this.registeredPort = self.getPort();
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.selfClient = new SelfClient();
        this.electionScheduler = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, "election-check-" + workerId);
                    thread.setDaemon(true);
                    return thread;
                });
        this.lotteryScheduler = Executors.newSingleThreadScheduledExecutor(
                runnable -> {
                    Thread thread = new Thread(runnable, "startup-lottery-" + workerId);
                    thread.setDaemon(true);
                    return thread;
                });
        this.jobExecutor = Executors.newFixedThreadPool(
                Math.max(WorkerClusterConfig.COORDINATOR_TERM_LIMIT + 1,
                        Runtime.getRuntime().availableProcessors()),
                runnable -> {
                    Thread thread = new Thread(runnable,
                            "job-" + workerId + "-" + jobThreadCounter.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                });
    }

    // ---------------------------------------------------------------- plumbing

    BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap() {
        return BootstrapServiceGrpc.newBlockingStub(channelPool.channel(bootstrapHost, bootstrapPort));
    }

    private static <T> void respond(StreamObserver<T> observer, T value) {
        observer.onNext(value);
        observer.onCompleted();
    }

    private static StatusRuntimeException invalidArgument(String message) {
        return Status.INVALID_ARGUMENT.withDescription(message).asRuntimeException();
    }

    private static StatusRuntimeException failedPrecondition(String message) {
        return Status.FAILED_PRECONDITION.withDescription(message).asRuntimeException();
    }

    private static StatusRuntimeException internalError(Throwable cause) {
        String message = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        return Status.INTERNAL.withDescription(message).withCause(cause).asRuntimeException();
    }

    private static StatusRuntimeException internalError(String message) {
        return Status.INTERNAL.withDescription(message).asRuntimeException();
    }

    // ---------------------------------------------------------------- remote handles

    /** Facade over a reachable worker, hiding whether it is this node or a gRPC peer. */
    private abstract class WorkerClient {
        abstract int workerId();

        abstract int priorityId();

        abstract int jac();

        abstract int recordAllocation();

        abstract List<WorkerInfo> neighbours();

        abstract int computePartialMax(List<Integer> numbers);

        abstract int countPrimes(List<Integer> numbers);

        abstract long sumPrimeRange(int start, int end);

        abstract void notifyTermEnd(int priorCoordinatorId, Map<Integer, Integer> finalJacTable);

        abstract void announceWinner(WinnerAnnouncement announcement);

        abstract ProcessedParticipants receiveElection(ElectionMessage message);

        abstract void setCurrentCoordinatorId(int coordinatorId);
    }

    private final class SelfClient extends WorkerClient {
        @Override
        int workerId() {
            return WorkerServiceImpl.this.workerId;
        }

        @Override
        int priorityId() {
            return priorityId;
        }

        @Override
        int jac() {
            return jobAllocationCounter.get();
        }

        @Override
        int recordAllocation() {
            return recordJobAllocationInternal();
        }

        @Override
        List<WorkerInfo> neighbours() {
            return neighbourListInternal();
        }

        @Override
        int computePartialMax(List<Integer> numbers) {
            return computePartialMaxInternal(numbers);
        }

        @Override
        int countPrimes(List<Integer> numbers) {
            return countPrimesInternal(numbers);
        }

        @Override
        long sumPrimeRange(int start, int end) {
            return sumPrimeRangeInternal(start, end);
        }

        @Override
        void notifyTermEnd(int priorCoordinatorId, Map<Integer, Integer> finalJacTable) {
            notifyTermEndInternal(priorCoordinatorId, finalJacTable);
        }

        @Override
        void announceWinner(WinnerAnnouncement announcement) {
            announceWinnerInternal(announcement);
        }

        @Override
        ProcessedParticipants receiveElection(ElectionMessage message) {
            return receiveElectionInternal(message);
        }

        @Override
        void setCurrentCoordinatorId(int coordinatorId) {
            recordCoordinator(coordinatorId);
        }
    }

    private final class RemoteClient extends WorkerClient {
        private final int id;
        private final String host;
        private final int port;
        private final WorkerServiceGrpc.WorkerServiceBlockingStub stub;

        RemoteClient(int id, String host, int port) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.stub = WorkerServiceGrpc.newBlockingStub(channelPool.channel(host, port));
        }

        @Override
        int workerId() {
            return id;
        }

        @Override
        int priorityId() {
            return stub.getPriorityId(EMPTY).getValue();
        }

        @Override
        int jac() {
            return stub.getJobAllocationCounter(EMPTY).getValue();
        }

        @Override
        int recordAllocation() {
            return stub.recordJobAllocation(EMPTY).getValue();
        }

        @Override
        List<WorkerInfo> neighbours() {
            return stub.getNeighbours(EMPTY).getWorkersList();
        }

        @Override
        int computePartialMax(List<Integer> numbers) {
            return stub.computePartialMax(NumberList.newBuilder().addAllNumbers(numbers).build()).getValue();
        }

        @Override
        int countPrimes(List<Integer> numbers) {
            return stub.countPrimes(NumberList.newBuilder().addAllNumbers(numbers).build()).getValue();
        }

        @Override
        long sumPrimeRange(int start, int end) {
            return stub.sumPrimeRange(RangeRequest.newBuilder().setStart(start).setEnd(end).build()).getValue();
        }

        @Override
        void notifyTermEnd(int priorCoordinatorId, Map<Integer, Integer> finalJacTable) {
            stub.notifyTermEnd(TermEndRequest.newBuilder()
                    .setPriorCoordinatorId(priorCoordinatorId)
                    .putAllFinalJacTable(finalJacTable)
                    .build());
        }

        @Override
        void announceWinner(WinnerAnnouncement announcement) {
            stub.announceWinner(announcement);
        }

        @Override
        ProcessedParticipants receiveElection(ElectionMessage message) {
            return stub.receiveElection(message);
        }

        @Override
        void setCurrentCoordinatorId(int coordinatorId) {
            stub.setCurrentCoordinatorId(IntValue.newBuilder().setValue(coordinatorId).build());
        }
    }

    private WorkerClient remoteClient(String host, int port, int id) {
        return new RemoteClient(id, host, port);
    }

    private WorkerClient remoteClient(WorkerInfo info) {
        return new RemoteClient(info.getWorkerId(), info.getHost(), info.getPort());
    }

    private WorkerClient remoteClient(CandidateInfo candidate) {
        return new RemoteClient(candidate.getWorkerId(), candidate.getHost(), candidate.getPort());
    }

    private int readWorkerId(WorkerClient worker) {
        try {
            return worker.workerId();
        } catch (RuntimeException e) {
            return Integer.MAX_VALUE;
        }
    }

    private int readPriority(WorkerClient worker) {
        try {
            return worker.priorityId();
        } catch (RuntimeException e) {
            return 0;
        }
    }

    private int readJAC(WorkerClient worker) {
        try {
            return worker.jac();
        } catch (RuntimeException e) {
            return Integer.MAX_VALUE;
        }
    }

    // ---------------------------------------------------------------- gRPC getters

    @Override
    public void getWorkerId(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(workerId).build());
    }

    @Override
    public void getPriorityId(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(priorityId).build());
    }

    @Override
    public void getJobAllocationCounter(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(jobAllocationCounter.get()).build());
    }

    @Override
    public void incrementJobAllocationCounter(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(recordJobAllocationInternal()).build());
    }

    @Override
    public void recordJobAllocation(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(recordJobAllocationInternal()).build());
    }

    @Override
    public void resetJobAllocationCounter(Empty request, StreamObserver<IntValue> observer) {
        jobAllocationCounter.set(0);
        respond(observer, IntValue.newBuilder().setValue(jobAllocationCounter.get()).build());
    }

    @Override
    public void getJobsThisTerm(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(completedJobsThisTerm.get()).build());
    }

    @Override
    public void getNeighbours(Empty request, StreamObserver<WorkerList> observer) {
        respond(observer, WorkerList.newBuilder().addAllWorkers(neighbourListInternal()).build());
    }

    @Override
    public void addNeighbour(WorkerInfo request, StreamObserver<Empty> observer) {
        if (request.getWorkerId() != workerId) {
            neighbours.add(request);
        }
        respond(observer, EMPTY);
    }

    @Override
    public void addNeighbourById(WorkerId request, StreamObserver<Empty> observer) {
        addNeighbourByIdInternal(request.getWorkerId());
        respond(observer, EMPTY);
    }

    @Override
    public void removeNeighbour(WorkerId request, StreamObserver<Empty> observer) {
        neighbours.removeIf(worker -> worker.getWorkerId() == request.getWorkerId());
        respond(observer, EMPTY);
    }

    @Override
    public void getCurrentCoordinatorId(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(currentCoordinatorId.get()).build());
    }

    @Override
    public void setCurrentCoordinatorId(IntValue request, StreamObserver<Empty> observer) {
        recordCoordinatorIfTermAllows(request.getValue(), "setCurrentCoordinatorId");
        respond(observer, EMPTY);
    }

    @Override
    public void getLeaderman(Empty request, StreamObserver<StringValue> observer) {
        respond(observer, StringValue.newBuilder().setValue(leaderman).build());
    }

    @Override
    public void syncNeighbours(Empty request, StreamObserver<IntValue> observer) {
        respond(observer, IntValue.newBuilder().setValue(refreshNeighboursInternal()).build());
    }

    @Override
    public void initiateElection(Empty request, StreamObserver<StringValue> observer) {
        try {
            respond(observer, StringValue.newBuilder().setValue(initiateElectionInternal()).build());
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    @Override
    public void receiveElection(ElectionMessage request, StreamObserver<ProcessedParticipants> observer) {
        try {
            respond(observer, receiveElectionInternal(request));
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    @Override
    public void announceWinner(WinnerAnnouncement request, StreamObserver<Empty> observer) {
        announceWinnerInternal(request);
        respond(observer, EMPTY);
    }

    @Override
    public void notifyTermEnd(TermEndRequest request, StreamObserver<Empty> observer) {
        notifyTermEndInternal(request.getPriorCoordinatorId(), request.getFinalJacTableMap());
        respond(observer, EMPTY);
    }

    @Override
    public void getTaskHistory(Empty request, StreamObserver<TaskHistory> observer) {
        respond(observer, TaskHistory.newBuilder().addAllTasks(new ArrayList<>(taskHistory)).build());
    }

    // ---------------------------------------------------------------- gRPC jobs

    @Override
    public void submitMaxJob(MaxJobRequest request, StreamObserver<TaskResult> observer) {
        try {
            respondTask(observer, submitMaxJobInternal(request.getClientId(), request.getNumbers().getNumbersList()));
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    @Override
    public void computePartialMax(NumberList request, StreamObserver<IntValue> observer) {
        try {
            respond(observer, IntValue.newBuilder().setValue(computePartialMaxInternal(request.getNumbersList())).build());
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    @Override
    public void submitPrimeCount(PrimeCountRequest request, StreamObserver<TaskResult> observer) {
        try {
            respondTask(observer, submitPrimeCountInternal(request.getClientId(), request.getNumbers().getNumbersList()));
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    @Override
    public void countPrimes(NumberList request, StreamObserver<IntValue> observer) {
        try {
            respond(observer, IntValue.newBuilder().setValue(countPrimesInternal(request.getNumbersList())).build());
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    @Override
    public void submitPrimeSum(PrimeSumRequest request, StreamObserver<TaskResult> observer) {
        try {
            respondTask(observer, submitPrimeSumInternal(request.getClientId(), request.getStart(), request.getEnd()));
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    @Override
    public void sumPrimeRange(RangeRequest request, StreamObserver<com.cs324.grpc.LongValue> observer) {
        try {
            respond(observer, com.cs324.grpc.LongValue.newBuilder()
                    .setValue(sumPrimeRangeInternal(request.getStart(), request.getEnd())).build());
        } catch (StatusRuntimeException e) {
            observer.onError(e);
        } catch (RuntimeException e) {
            observer.onError(internalError(e));
        }
    }

    // ---------------------------------------------------------------- internal state

    private void respondTask(StreamObserver<TaskResult> observer, TaskResult result) {
        respond(observer, result);
    }

    private int recordJobAllocationInternal() {
        int updated = jobAllocationCounter.incrementAndGet();
        System.out.println("[Worker " + workerId + "] JAC changed to " + updated
                + " (recorded a distributed job segment allocation)");
        return updated;
    }

    private List<WorkerInfo> neighbourListInternal() {
        return new ArrayList<>(neighbours);
    }

    private void addNeighbourByIdInternal(int targetWorkerId) {
        if (targetWorkerId == workerId) {
            return;
        }
        try {
            for (WorkerInfo info : bootstrap().getActiveWorkers(EMPTY).getWorkersList()) {
                if (info.getWorkerId() == targetWorkerId) {
                    neighbours.add(info);
                    return;
                }
            }
        } catch (RuntimeException e) {
            System.err.println("[Worker " + workerId + "] cannot resolve worker " + targetWorkerId
                    + " via Bootstrap Node: " + e.getMessage());
            return;
        }
        System.err.println("[Worker " + workerId + "] cannot resolve worker " + targetWorkerId
                + " via Bootstrap Node - it is not registered");
    }

    /**
     * Atomically records the current coordinator. Whenever this node newly
     * becomes the coordinator, a fresh term begins: the per-term accepted and
     * completed submitted-job counters reset to zero. Repeated announcements for
     * the same coordinator must not reset an in-progress term.
     */
    private void recordCoordinator(int coordinatorId) {
        int previous = currentCoordinatorId.getAndSet(coordinatorId);
        boolean startsNewLocalTerm = coordinatorId == workerId && previous != workerId;
        if (startsNewLocalTerm) {
            jobsThisTerm.set(0);
            completedJobsThisTerm.set(0);
            termEndScheduled.set(false);
        }
        if (coordinatorId != WorkerClusterConfig.NO_COORDINATOR) {
            electionVacancyAuthorized.set(false);
        }
        System.out.println("[Worker " + workerId + "] coordinator set to " + coordinatorId
                + (previous == coordinatorId ? "" : " (was " + previous + ")"));
        if (startsNewLocalTerm) {
            System.out.println("[Worker " + workerId + "] ANNOUNCEMENT: I am the coordinator");
        } else if (coordinatorId != WorkerClusterConfig.NO_COORDINATOR) {
            System.out.println("[Worker " + workerId + "] ANNOUNCEMENT: coordinator is worker " + coordinatorId);
        }
    }

    private boolean recordCoordinatorIfTermAllows(int coordinatorId, String source) {
        int current = currentCoordinatorId.get();
        if (current != WorkerClusterConfig.NO_COORDINATOR && current != coordinatorId) {
            System.out.println("[Worker " + workerId + "] ignoring coordinator change from " + current
                    + " to " + coordinatorId + " from " + source
                    + " because the active coordinator term has not ended after 5 jobs");
            return false;
        }
        recordCoordinator(coordinatorId);
        return true;
    }

    private CandidateInfo selfCandidate() {
        return CandidateInfo.newBuilder()
                .setWorkerId(workerId)
                .setJac(jobAllocationCounter.get())
                .setPriorityId(priorityId)
                .setHost(registeredHost)
                .setPort(registeredPort)
                .build();
    }

    // ---------------------------------------------------------------- election protocol

    private String initiateElectionInternal() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return "Election already in progress - ignoring new election request";
        }
        try {
            int existing = currentCoordinatorId.get();
            if (existing != WorkerClusterConfig.NO_COORDINATOR) {
                return "Coordinator already present: worker " + existing
                        + " - no election started (an election runs only when no coordinator "
                        + "is active, e.g. after a 5-job term ends)";
            }
            int liveCoordinator = discoverLiveCoordinator();
            if (liveCoordinator != WorkerClusterConfig.NO_COORDINATOR) {
                recordCoordinator(liveCoordinator);
                return "Coordinator already present: worker " + liveCoordinator
                        + " - no election started because a live coordinator was discovered";
            }
            if (!electionVacancyAuthorized.get()) {
                return "Election not authorized - the active coordinator has not completed "
                        + WorkerClusterConfig.COORDINATOR_TERM_LIMIT
                        + " submitted jobs and has not failed";
            }

            refreshNeighboursInternal();

            CandidateInfo self = selfCandidate();
            String electionId = UUID.randomUUID().toString();
            processedElectionIds.add(electionId);

            System.out.println("[Worker " + workerId + "] election " + electionId + " started, JAC=" + self.getJac()
                    + ", neighbours=" + neighbours.size());

            ElectionMessage message = ElectionMessage.newBuilder()
                    .setElectionId(electionId)
                    .setOriginator(self)
                    .setSender(self)
                    .build();
            ProcessedParticipants collected = propagateToNeighbours(message);

            Map<Integer, CandidateInfo> allParticipants = new LinkedHashMap<>();
            for (CandidateInfo candidate : collected.getParticipantsList()) {
                allParticipants.putIfAbsent(candidate.getWorkerId(), candidate);
            }
            allParticipants.putIfAbsent(self.getWorkerId(), self);

            CandidateInfo winner = selectWinner(allParticipants.values());
            recordCoordinator(winner.getWorkerId());
            processedCoordinatorAnnouncements.add(electionId);

            System.out.println("[Worker " + workerId + "] election " + electionId + " winner: worker "
                    + winner.getWorkerId() + " (JAC=" + winner.getJac() + ") across "
                    + allParticipants.size() + " reachable workers");

            WinnerAnnouncement announcement = WinnerAnnouncement.newBuilder()
                    .setElectionId(electionId)
                    .setWinner(winner)
                    .addAllParticipants(allParticipants.values())
                    .build();
            propagateCoordinatorAnnouncement(announcement);

            return "Coordinator elected: worker " + winner.getWorkerId() + " (JAC=" + winner.getJac()
                    + ") across " + allParticipants.size() + " reachable workers [electionId=" + electionId + "]";
        } finally {
            electionInProgress.set(false);
        }
    }

    private ProcessedParticipants receiveElectionInternal(ElectionMessage message) {
        if (message == null || message.getElectionId().isEmpty()) {
            return ProcessedParticipants.getDefaultInstance();
        }
        if (!processedElectionIds.add(message.getElectionId())) {
            System.out.println("[Worker " + workerId + "] dropping duplicate election message "
                    + message.getElectionId() + " from worker " + message.getSender().getWorkerId());
            return ProcessedParticipants.newBuilder().setElectionId(message.getElectionId()).build();
        }

        boolean wasInProgress = electionInProgress.getAndSet(true);
        try {
            refreshNeighboursInternal();

            CandidateInfo self = selfCandidate();
            Map<Integer, CandidateInfo> participants = new LinkedHashMap<>();
            for (CandidateInfo candidate : message.getParticipantsList()) {
                participants.putIfAbsent(candidate.getWorkerId(), candidate);
            }
            participants.putIfAbsent(self.getWorkerId(), self);

            ElectionMessage forwarded = ElectionMessage.newBuilder()
                    .setElectionId(message.getElectionId())
                    .setOriginator(message.getOriginator())
                    .setSender(self)
                    .addAllParticipants(participants.values())
                    .build();
            return propagateToNeighbours(forwarded);
        } finally {
            electionInProgress.set(wasInProgress);
        }
    }

    /**
     * Forwards a message to every neighbour except the hop it came from,
     * merging the participant sets of all echo replies. Deduplication is by
     * worker id, so a worker mentioned on several paths is counted once.
     */
    private ProcessedParticipants propagateToNeighbours(ElectionMessage message) {
        Map<Integer, CandidateInfo> participants = new LinkedHashMap<>();
        for (CandidateInfo candidate : message.getParticipantsList()) {
            participants.putIfAbsent(candidate.getWorkerId(), candidate);
        }
        CandidateInfo sender = message.getSender();
        int senderId = sender.getWorkerId();
        for (WorkerInfo neighbour : neighbours) {
            if (neighbour.getWorkerId() == senderId) {
                continue;
            }
            try {
                ProcessedParticipants reply = remoteClient(neighbour).receiveElection(message);
                if (reply != null) {
                    for (CandidateInfo candidate : reply.getParticipantsList()) {
                        participants.putIfAbsent(candidate.getWorkerId(), candidate);
                    }
                }
            } catch (RuntimeException e) {
                System.err.println("[Worker " + workerId + "] election propagation to " + neighbour
                        + " failed: " + e.getMessage());
            }
        }
        return ProcessedParticipants.newBuilder()
                .setElectionId(message.getElectionId())
                .setSender(sender)
                .addAllParticipants(participants.values())
                .build();
    }

    private CandidateInfo selectWinner(Collection<CandidateInfo> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            throw internalError("election produced no candidates");
        }
        List<CandidateInfo> eligibleCandidates = new ArrayList<>(candidates);
        int previousCoordinator = previousCoordinatorId.get();
        if (previousCoordinator != WorkerClusterConfig.NO_COORDINATOR && eligibleCandidates.size() > 1) {
            List<CandidateInfo> rotated = eligibleCandidates.stream()
                    .filter(candidate -> candidate.getWorkerId() != previousCoordinator)
                    .toList();
            if (!rotated.isEmpty()) {
                eligibleCandidates = rotated;
            }
        }

        int lowestJac = eligibleCandidates.stream()
                .mapToInt(CandidateInfo::getJac)
                .min()
                .orElseThrow(() -> internalError("election produced no candidates"));
        List<CandidateInfo> eligible = eligibleCandidates.stream()
                .filter(candidate -> candidate.getJac() == lowestJac)
                .sorted(Comparator.comparingInt(CandidateInfo::getWorkerId))
                .toList();
        return eligible.get(0);
    }

    private void propagateCoordinatorAnnouncement(WinnerAnnouncement announcement) {
        for (WorkerInfo neighbour : neighbours) {
            if (neighbour.getWorkerId() == workerId) {
                continue;
            }
            try {
                remoteClient(neighbour).announceWinner(announcement);
                System.out.println("[Worker " + workerId + "] COORDINATOR message propagated to worker "
                        + neighbour.getWorkerId());
            } catch (RuntimeException e) {
                System.err.println("[Worker " + workerId + "] could not propagate COORDINATOR message to worker "
                        + neighbour.getWorkerId() + ": " + e.getMessage());
            }
        }
    }

    private void announceWinnerInternal(WinnerAnnouncement announcement) {
        if (announcement == null || !announcement.hasWinner()) {
            return;
        }
        if (!processedCoordinatorAnnouncements.add(announcement.getElectionId())) {
            System.out.println("[Worker " + workerId + "] dropping duplicate COORDINATOR message "
                    + announcement.getElectionId());
            return;
        }
        processedElectionIds.add(announcement.getElectionId());
        int winnerId = announcement.getWinner().getWorkerId();
        if (!recordCoordinatorIfTermAllows(winnerId, "COORDINATOR message " + announcement.getElectionId())) {
            return;
        }
        System.out.println("[Worker " + workerId + "] recorded coordinator worker "
                + winnerId + " (JAC=" + announcement.getWinner().getJac()
                + ") for election " + announcement.getElectionId());
        propagateCoordinatorAnnouncement(announcement);
    }

    private void notifyTermEndInternal(int priorCoordinatorId, Map<Integer, Integer> finalJacTable) {
        System.out.println("[Worker " + workerId + "] received Term_End from coordinator worker "
                + priorCoordinatorId + " -> final JAC table: " + finalJacTable);
        previousCoordinatorId.set(priorCoordinatorId);
        if (currentCoordinatorId.compareAndSet(priorCoordinatorId, WorkerClusterConfig.NO_COORDINATOR)) {
            electionVacancyAuthorized.set(true);
        }
    }

    // ---------------------------------------------------------------- neighbours

    private synchronized int refreshNeighboursInternal() {
        try {
            List<WorkerInfo> active = new ArrayList<>(bootstrap().getActiveWorkers(EMPTY).getWorkersList());
            boolean foundSelf = active.stream().anyMatch(info -> info.getWorkerId() == workerId);
            if (!foundSelf) {
                active.add(selfInfo);
            }
            if (active.size() <= 1) {
                neighbours.clear();
                return 0;
            }

            active.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));

            neighbours.clear();
            for (WorkerInfo peer : active) {
                if (peer.getWorkerId() != workerId) {
                    neighbours.add(peer);
                }
            }
            System.out.println("[Worker " + workerId + "] connected to all other worker nodes "
                    + "(neighbours=" + neighbours.size() + "): " + neighbours);

            return neighbours.size();
        } catch (RuntimeException e) {
            System.err.println("[Worker " + workerId + "] neighbour sync failed: " + e.getMessage());
            return neighbours.size();
        }
    }

    // ---------------------------------------------------------------- startup lottery

    public void startAutoElectionChecks() {
        long initialDelay = Math.max(FIRST_WORKER_DELAY_MS, (long) workerId * WORKER_DELAY_STEP_MS);
        electionScheduler.scheduleWithFixedDelay(this::autoElectionTick,
                initialDelay, AUTO_ELECTION_PERIOD_MS, TimeUnit.MILLISECONDS);
        System.out.println("[Worker " + workerId + "] auto-election checks started (first check in "
                + initialDelay + " ms)");
    }

    private static final long FIRST_WORKER_DELAY_MS = 2_500;
    private static final long WORKER_DELAY_STEP_MS = 2_000;
    private static final long AUTO_ELECTION_PERIOD_MS = 4_000;

    private static final long LOTTERY_MIN_DELAY_MS = 150;
    private static final int LOTTERY_SPAN_MS = 150;
    private static final long LOTTERY_POLL_MS = 200;
    private static final int SEGMENT_NUMBERS = 3;
    private static final int SEGMENT_RANGE_LENGTH = 200;

    public void startStartupLottery() {
        lotteryScheduler.scheduleWithFixedDelay(this::lotteryTick,
                LOTTERY_MIN_DELAY_MS, LOTTERY_POLL_MS, TimeUnit.MILLISECONDS);
        System.out.println("[Worker " + workerId + "] startup lottery started (randomized countdown "
                + LOTTERY_MIN_DELAY_MS + "-" + (LOTTERY_MIN_DELAY_MS + LOTTERY_SPAN_MS) + " ms)");
    }

    private void lotteryTick() {
        try {
            if (priorityId == 0) {
                if (lotteryFireAt == -1L) {
                    long completeTime = readClusterCompleteTime();
                    int expectedClusterSize = readClusterSize();
                    boolean fullCluster = readActiveWorkerCount() >= expectedClusterSize;
                    if (completeTime > 0 && fullCluster) {
                        lotteryFireAt = completeTime + lotteryCountdown();
                        System.out.println("[Worker " + workerId + "] lottery countdown -> "
                                + (lotteryFireAt - System.currentTimeMillis())
                                + " ms (cluster complete at " + completeTime + ")");
                    } else {
                        return;
                    }
                }
                if (System.currentTimeMillis() < lotteryFireAt) {
                    return;
                }
                int assigned = bootstrap().lotteryClaim(WorkerId.newBuilder().setWorkerId(workerId).build())
                        .getValue();
                priorityId = assigned;
                System.out.println("[Worker " + workerId + "] lottery claim -> startup priority "
                        + assigned + " (Bootstrap only sequences startup claims; "
                        + "workers still elect the coordinator)");
            }

            if (priorityId != 0 && isPriorityTableComplete() && !neighboursSyncedAfterLottery) {
                neighboursSyncedAfterLottery = true;
                refreshNeighboursInternal();
                System.out.println("[Worker " + workerId + "] rebuilt neighbour ring for the full cluster "
                        + "(neighbours=" + neighbours.size() + ")");
            }

        } catch (RuntimeException e) {
            System.err.println("[Worker " + workerId + "] lottery check failed: " + e.getMessage());
        }
    }

    private static long lotteryCountdown() {
        return LOTTERY_MIN_DELAY_MS + ThreadLocalRandom.current().nextInt(LOTTERY_SPAN_MS + 1);
    }

    private long readClusterCompleteTime() {
        try {
            return bootstrap().getClusterCompleteTime(EMPTY).getValue();
        } catch (RuntimeException e) {
            return -1L;
        }
    }

    private int readActiveWorkerCount() {
        try {
            return bootstrap().getActiveWorkers(EMPTY).getWorkersCount();
        } catch (RuntimeException e) {
            return 0;
        }
    }

    private int readClusterSize() {
        try {
            return bootstrap().getClusterSize(EMPTY).getValue();
        } catch (RuntimeException e) {
            return WorkerClusterConfig.WORKER_COUNT;
        }
    }

    private boolean isPriorityTableComplete() {
        try {
            return bootstrap().getPriorityTable(EMPTY).getPrioritiesCount() >= readClusterSize();
        } catch (RuntimeException e) {
            return false;
        }
    }

    private void autoElectionTick() {
        int knownCoordinator = currentCoordinatorId.get();
        if (knownCoordinator != WorkerClusterConfig.NO_COORDINATOR && !isCoordinatorAlive(knownCoordinator)) {
            if (currentCoordinatorId.compareAndSet(knownCoordinator, WorkerClusterConfig.NO_COORDINATOR)) {
                previousCoordinatorId.set(knownCoordinator);
                electionVacancyAuthorized.set(true);
                System.out.println("[Worker " + workerId + "] coordinator worker " + knownCoordinator
                        + " is no longer active - triggering re-election");
            }
        }
        if (currentCoordinatorId.get() != WorkerClusterConfig.NO_COORDINATOR || electionInProgress.get()) {
            return;
        }
        if (readActiveWorkerCount() == 0) {
            return;
        }
        if (workerId != highestActiveWorkerId()) {
            return;
        }
        try {
            String result = initiateElectionInternal();
            if (!result.startsWith("Coordinator already present")
                    && !result.startsWith("Election not authorized")) {
                System.out.println("[Worker " + workerId + "] auto-election result: " + result);
            }
        } catch (RuntimeException e) {
            System.err.println("[Worker " + workerId + "] auto-election failed: " + e.getMessage());
        }
    }

    private int highestActiveWorkerId() {
        try {
            int highest = workerId;
            for (WorkerInfo worker : bootstrap().getActiveWorkers(EMPTY).getWorkersList()) {
                highest = Math.max(highest, worker.getWorkerId());
            }
            return highest;
        } catch (RuntimeException e) {
            return workerId;
        }
    }

    private boolean isWorkerActive(int targetWorkerId) {
        if (targetWorkerId == workerId) {
            return true;
        }
        try {
            for (WorkerInfo worker : bootstrap().getActiveWorkers(EMPTY).getWorkersList()) {
                if (worker.getWorkerId() == targetWorkerId) {
                    return true;
                }
            }
        } catch (RuntimeException e) {
            return false;
        }
        return false;
    }

    private boolean isCoordinatorAlive(int coordinatorId) {
        if (coordinatorId == workerId) {
            return true;
        }
        WorkerInfo coordinatorInfo = findWorkerInfo(coordinatorId);
        if (coordinatorInfo == null) {
            return false;
        }
        try {
            int reported = workerProbe(coordinatorInfo)
                    .withDeadlineAfter(1, TimeUnit.SECONDS)
                    .getCurrentCoordinatorId(EMPTY)
                    .getValue();
            return reported == coordinatorId;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private int discoverLiveCoordinator() {
        int discovered = discoverLiveCoordinatorFrom(neighbourListInternal());
        if (discovered != WorkerClusterConfig.NO_COORDINATOR) {
            return discovered;
        }
        try {
            return discoverLiveCoordinatorFrom(bootstrap().getActiveWorkers(EMPTY).getWorkersList());
        } catch (RuntimeException e) {
            return WorkerClusterConfig.NO_COORDINATOR;
        }
    }

    private int discoverLiveCoordinatorFrom(List<WorkerInfo> workers) {
        if (workers == null) {
            return WorkerClusterConfig.NO_COORDINATOR;
        }
        for (WorkerInfo worker : workers) {
            int candidateId = worker.getWorkerId();
            if (candidateId == workerId) {
                continue;
            }
            try {
                int reportedCoordinator = workerProbe(worker)
                        .withDeadlineAfter(1, TimeUnit.SECONDS)
                        .getCurrentCoordinatorId(EMPTY)
                        .getValue();
                if (reportedCoordinator != WorkerClusterConfig.NO_COORDINATOR
                        && isCoordinatorKnownReachable(reportedCoordinator, workers)) {
                    return reportedCoordinator;
                }
            } catch (RuntimeException ignored) {
                // Ignore a stale worker and keep looking for an active coordinator.
            }
        }
        return WorkerClusterConfig.NO_COORDINATOR;
    }

    private boolean isCoordinatorKnownReachable(int coordinatorId, List<WorkerInfo> knownWorkers) {
        if (coordinatorId == workerId) {
            return true;
        }
        WorkerInfo coordinatorInfo = null;
        if (knownWorkers != null) {
            for (WorkerInfo worker : knownWorkers) {
                if (worker.getWorkerId() == coordinatorId) {
                    coordinatorInfo = worker;
                    break;
                }
            }
        }
        if (coordinatorInfo == null) {
            coordinatorInfo = findWorkerInfo(coordinatorId);
        }
        if (coordinatorInfo == null) {
            return false;
        }
        try {
            int reported = workerProbe(coordinatorInfo)
                    .withDeadlineAfter(1, TimeUnit.SECONDS)
                    .getCurrentCoordinatorId(EMPTY)
                    .getValue();
            return reported == coordinatorId;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private WorkerInfo findWorkerInfo(int targetWorkerId) {
        for (WorkerInfo worker : neighbours) {
            if (worker.getWorkerId() == targetWorkerId) {
                return worker;
            }
        }
        try {
            for (WorkerInfo worker : bootstrap().getActiveWorkers(EMPTY).getWorkersList()) {
                if (worker.getWorkerId() == targetWorkerId) {
                    return worker;
                }
            }
        } catch (RuntimeException ignored) {
            // Caller will treat this as not found.
        }
        return null;
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub workerProbe(WorkerInfo worker) {
        return WorkerServiceGrpc.newBlockingStub(channelPool.channel(worker.getHost(), worker.getPort()));
    }

    // ---------------------------------------------------------------- jobs

    private TaskResult submitMaxJobInternal(String clientId, List<Integer> numbers) {
        if (numbers == null || numbers.isEmpty()) {
            throw invalidArgument("numbers must not be null or empty");
        }
        requireCoordinator();

        int termSlot = claimTermSlot();
        String taskId = UUID.randomUUID().toString();
        return runJob(termSlot, () -> executeSubmitMaxJob(taskId, clientId, numbers));
    }

    private <T> T runJob(int termSlot, Callable<T> job) {
        System.out.println("[Worker " + workerId + "] accepted submitted job #" + termSlot
                + "/" + WorkerClusterConfig.COORDINATOR_TERM_LIMIT + " for this coordinator term");
        try {
            T result = jobExecutor.submit(() -> {
                System.out.println("[Worker " + workerId + "] executing submitted job #" + termSlot
                        + " on thread " + Thread.currentThread().getName());
                return job.call();
            }).get();
            markCompletedJob(termSlot);
            return result;
        } catch (ExecutionException e) {
            releaseTermSlot(termSlot);
            throw unwrapExecution(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            releaseTermSlot(termSlot);
            throw internalError("Job interrupted");
        }
    }

    private void markCompletedJob(int termSlot) {
        int completed = completedJobsThisTerm.incrementAndGet();
        System.out.println("[Worker " + workerId + "] completed submitted job #" + completed
                + "/" + WorkerClusterConfig.COORDINATOR_TERM_LIMIT
                + " for this coordinator term (accepted slot " + termSlot + ")");
    }

    private void releaseTermSlot(int termSlot) {
        jobsThisTerm.updateAndGet(current -> Math.max(0, current - 1));
        System.out.println("[Worker " + workerId + "] submitted job slot " + termSlot
                + " did not complete successfully - it does not count toward the 5 completed jobs");
    }

    private StatusRuntimeException unwrapExecution(ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof StatusRuntimeException statusRuntimeException) {
            return statusRuntimeException;
        }
        return internalError(cause == null ? e : cause);
    }

    private void requireCoordinator() {
        if (currentCoordinatorId.get() != workerId) {
            throw failedPrecondition("Worker " + workerId
                    + " is not the coordinator. Current coordinator is " + currentCoordinatorId.get());
        }
    }

    /**
     * Reserves one of the coordinator's per-term submitted-job slots. An atomic
     * check-then-increment keeps the counter at most
     * {@link WorkerClusterConfig#COORDINATOR_TERM_LIMIT} for the current term.
     */
    private int claimTermSlot() {
        for (; ; ) {
            int current = jobsThisTerm.get();
            if (current >= WorkerClusterConfig.COORDINATOR_TERM_LIMIT) {
                if (hasCompletedFiveJobs() && termEndScheduled.compareAndSet(false, true)) {
                    endCoordinatorTerm();
                }
                throw failedPrecondition("Coordinator term ended after "
                        + WorkerClusterConfig.COORDINATOR_TERM_LIMIT + " jobs; no jobs can be processed "
                        + "until the next leader election completes");
            }
            if (jobsThisTerm.compareAndSet(current, current + 1)) {
                return current + 1;
            }
        }
    }

    private void endCoordinatorTerm() {
        if (!hasCompletedFiveJobs()) {
            termEndScheduled.set(false);
            return;
        }
        if (!currentCoordinatorId.compareAndSet(workerId, WorkerClusterConfig.NO_COORDINATOR)) {
            return;
        }
        previousCoordinatorId.set(workerId);
        electionVacancyAuthorized.set(true);
        System.out.println("[Worker " + workerId + "] completed "
                + WorkerClusterConfig.COORDINATOR_TERM_LIMIT + " jobs this term - ending term "
                + "and starting a new leader election");
        broadcastTermEnd();
        System.out.println("[Worker " + workerId + "] term ended with JAC="
                + jobAllocationCounter.get());

        try {
            String result = initiateElectionInternal();
            System.out.println("[Worker " + workerId + "] term re-election result: " + result);
        } catch (RuntimeException e) {
            System.err.println("[Worker " + workerId + "] term re-election failed: " + e.getMessage());
        }
    }

    private boolean hasCompletedFiveJobs() {
        return completedJobsThisTerm.get() == WorkerClusterConfig.COORDINATOR_TERM_LIMIT;
    }

    /**
     * Phase-3 step-down protocol: the coordinator broadcasts a Term_End message
     * with the final verified JAC table to every reachable worker.
     */
    private Map<Integer, Integer> broadcastTermEnd() {
        List<WorkerClient> reachable = getReachableWorkerServices();
        Map<Integer, Integer> jacTable = new LinkedHashMap<>();
        for (WorkerClient worker : reachable) {
            jacTable.put(readWorkerId(worker), readJAC(worker));
        }
        System.out.println("[Worker " + workerId + "] Term_End broadcast -> final JAC table: " + jacTable);
        for (WorkerClient worker : reachable) {
            int targetId = readWorkerId(worker);
            if (targetId == workerId) {
                continue;
            }
            try {
                worker.notifyTermEnd(workerId, jacTable);
            } catch (RuntimeException e) {
                System.err.println("[Worker " + workerId + "] could not deliver Term_End to worker "
                        + targetId + ": " + e.getMessage());
            }
        }
        return jacTable;
    }

    private TaskResult executeSubmitMaxJob(String taskId, String clientId, List<Integer> numbers) {
        List<WorkerClient> assignees = getAssignmentWorkers();
        int segmentCount = segmentCountFor(assignees.size(), numbers.size(), SEGMENT_NUMBERS);
        if (segmentCount == 0) {
            throw internalError("No reachable workers are available for MAX job");
        }

        long startedAt = System.currentTimeMillis();
        List<Integer> usedWorkers = new ArrayList<>();
        System.out.println("[Worker " + workerId + "] MAX task " + taskId + " (" + clientId
                + ") -> numbers=" + numbers.size() + " items, reachableWorkers=" + assignees.size()
                + ", segments=" + segmentCount);

        int finalMax = Integer.MIN_VALUE;
        int start = 0;
        for (int index = 0; index < segmentCount; index++) {
            int remainingNumbers = numbers.size() - start;
            int remainingSegments = segmentCount - index;
            int chunkSize = (remainingNumbers + remainingSegments - 1) / remainingSegments;
            List<Integer> chunk = new ArrayList<>(numbers.subList(start, start + chunkSize));
            WorkerClient worker = assignees.get(index);
            int targetId = readWorkerId(worker);
            usedWorkers.add(targetId);

            try {
                worker.recordAllocation();
                System.out.println("[Worker " + workerId + "] MAX task " + taskId + " segment "
                        + (index + 1) + "/" + segmentCount + " -> toWorkerId=" + targetId
                        + ", section=" + chunk);
                int partialMax = worker.computePartialMax(chunk);
                finalMax = Math.max(finalMax, partialMax);
                System.out.println("[Worker " + workerId + "] MAX task " + taskId + " partial <- "
                        + targetId + ": " + partialMax + ", running=" + finalMax);
            } catch (RuntimeException e) {
                throw internalError("MAX job failed while assigning worker " + targetId);
            }

            start += chunkSize;
        }

        TaskResult record = TaskResult.newBuilder()
                .setTaskId(taskId)
                .setClientId(clientId)
                .setJobType("MAX")
                .setResult(finalMax)
                .setSegmentCount(segmentCount)
                .addAllWorkerIds(usedWorkers)
                .setCoordinatorId(workerId)
                .setStartedAtMillis(startedAt)
                .setCompletedAtMillis(System.currentTimeMillis())
                .build();
        recordTask(record);
        System.out.println("[Worker " + workerId + "] MAX task " + taskId + " complete -> result="
                + finalMax + ", workers=" + usedWorkers);
        return record;
    }

    private int computePartialMaxInternal(List<Integer> numbers) {
        if (numbers == null || numbers.isEmpty()) {
            throw invalidArgument("numbers must not be null or empty");
        }
        return runPartialJob(() -> {
            int partialMax = Collections.max(numbers);
            System.out.println("[Worker " + workerId + "] MAX partial compute -> section="
                    + numbers + ", partialMax=" + partialMax);
            return partialMax;
        });
    }

    private <T> T runPartialJob(Callable<T> job) {
        try {
            return jobExecutor.submit(job).get();
        } catch (ExecutionException e) {
            throw unwrapExecution(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw internalError("Job interrupted");
        }
    }

    private TaskResult submitPrimeCountInternal(String clientId, List<Integer> numbers) {
        if (numbers == null || numbers.isEmpty()) {
            throw invalidArgument("numbers must not be null or empty");
        }
        requireCoordinator();

        int termSlot = claimTermSlot();
        String taskId = UUID.randomUUID().toString();
        return runJob(termSlot, () -> executeSubmitPrimeCount(taskId, clientId, numbers));
    }

    private TaskResult executeSubmitPrimeCount(String taskId, String clientId, List<Integer> numbers) {
        List<WorkerClient> assignees = getAssignmentWorkers();
        int segmentCount = segmentCountFor(assignees.size(), numbers.size(), SEGMENT_NUMBERS);
        if (segmentCount == 0) {
            throw internalError("No reachable workers are available for PRIMECOUNT job");
        }

        long startedAt = System.currentTimeMillis();
        List<Integer> usedWorkers = new ArrayList<>();
        System.out.println("[Worker " + workerId + "] PRIMECOUNT task " + taskId + " (" + clientId
                + ") -> numbers=" + numbers.size() + " items, reachableWorkers=" + assignees.size()
                + ", segments=" + segmentCount);

        int totalPrimes = 0;
        int start = 0;
        for (int index = 0; index < segmentCount; index++) {
            int remainingNumbers = numbers.size() - start;
            int remainingSegments = segmentCount - index;
            int chunkSize = (remainingNumbers + remainingSegments - 1) / remainingSegments;
            List<Integer> chunk = new ArrayList<>(numbers.subList(start, start + chunkSize));
            WorkerClient worker = assignees.get(index);
            int targetId = readWorkerId(worker);
            usedWorkers.add(targetId);

            try {
                worker.recordAllocation();
                System.out.println("[Worker " + workerId + "] PRIMECOUNT task " + taskId + " segment "
                        + (index + 1) + "/" + segmentCount + " -> toWorkerId=" + targetId
                        + ", section=" + chunk);
                int partialCount = worker.countPrimes(chunk);
                totalPrimes += partialCount;
                System.out.println("[Worker " + workerId + "] PRIMECOUNT task " + taskId + " partial <- "
                        + targetId + ": " + partialCount + ", running=" + totalPrimes);
            } catch (RuntimeException e) {
                throw internalError("PRIMECOUNT job failed while assigning worker " + targetId);
            }

            start += chunkSize;
        }

        TaskResult record = TaskResult.newBuilder()
                .setTaskId(taskId)
                .setClientId(clientId)
                .setJobType("PRIMECOUNT")
                .setResult(totalPrimes)
                .setSegmentCount(segmentCount)
                .addAllWorkerIds(usedWorkers)
                .setCoordinatorId(workerId)
                .setStartedAtMillis(startedAt)
                .setCompletedAtMillis(System.currentTimeMillis())
                .build();
        recordTask(record);
        System.out.println("[Worker " + workerId + "] PRIMECOUNT task " + taskId + " complete -> result="
                + totalPrimes + ", workers=" + usedWorkers);
        return record;
    }

    private int countPrimesInternal(List<Integer> numbers) {
        if (numbers == null) {
            throw invalidArgument("numbers must not be null");
        }
        return runPartialJob(() -> {
            int count = 0;
            for (Integer value : numbers) {
                if (value == null) {
                    throw invalidArgument("numbers must not contain null values");
                }
                if (isPrime(value)) {
                    count++;
                }
            }
            System.out.println("[Worker " + workerId + "] PRIMECOUNT partial compute -> section="
                    + numbers + ", primes=" + count);
            return count;
        });
    }

    private TaskResult submitPrimeSumInternal(String clientId, int start, int end) {
        if (start < 1 || end < start) {
            throw invalidArgument("invalid range: start=" + start + ", end=" + end);
        }
        requireCoordinator();

        int termSlot = claimTermSlot();
        String taskId = UUID.randomUUID().toString();
        return runJob(termSlot, () -> executeSubmitPrimeSum(taskId, clientId, start, end));
    }

    private TaskResult executeSubmitPrimeSum(String taskId, String clientId, int start, int end) {
        List<WorkerClient> assignees = getAssignmentWorkers();
        int segmentCount = segmentCountFor(assignees.size(), end - start + 1, SEGMENT_RANGE_LENGTH);
        if (segmentCount == 0) {
            throw internalError("No reachable workers are available for PRIMESUM job");
        }

        long startedAt = System.currentTimeMillis();
        List<Integer> usedWorkers = new ArrayList<>();
        System.out.println("[Worker " + workerId + "] PRIMESUM task " + taskId + " (" + clientId
                + ") -> range=[" + start + ", " + end + "], reachableWorkers=" + assignees.size()
                + ", segments=" + segmentCount);

        long totalSum = 0;
        int segmentStart = start;
        for (int index = 0; index < segmentCount; index++) {
            int remainingLength = end - segmentStart + 1;
            int remainingSegments = segmentCount - index;
            int chunkSize = (remainingLength + remainingSegments - 1) / remainingSegments;
            int segmentEnd = segmentStart + chunkSize - 1;

            WorkerClient worker = assignees.get(index);
            int targetId = readWorkerId(worker);
            usedWorkers.add(targetId);

            try {
                worker.recordAllocation();
                System.out.println("[Worker " + workerId + "] PRIMESUM task " + taskId + " segment "
                        + (index + 1) + "/" + segmentCount + " -> toWorkerId=" + targetId
                        + ", range=[" + segmentStart + ", " + segmentEnd + "]");
                long partialSum = worker.sumPrimeRange(segmentStart, segmentEnd);
                totalSum += partialSum;
                System.out.println("[Worker " + workerId + "] PRIMESUM task " + taskId + " partial <- "
                        + targetId + ": " + partialSum + ", running=" + totalSum);
            } catch (RuntimeException e) {
                throw internalError("PRIMESUM job failed while assigning worker " + targetId);
            }

            segmentStart = segmentEnd + 1;
        }

        TaskResult record = TaskResult.newBuilder()
                .setTaskId(taskId)
                .setClientId(clientId)
                .setJobType("PRIMESUM")
                .setResult(totalSum)
                .setSegmentCount(segmentCount)
                .addAllWorkerIds(usedWorkers)
                .setCoordinatorId(workerId)
                .setStartedAtMillis(startedAt)
                .setCompletedAtMillis(System.currentTimeMillis())
                .build();
        recordTask(record);
        System.out.println("[Worker " + workerId + "] PRIMESUM task " + taskId + " complete -> result="
                + totalSum + ", workers=" + usedWorkers);
        return record;
    }

    private long sumPrimeRangeInternal(int start, int end) {
        if (start < 1 || end < start) {
            throw invalidArgument("invalid range: start=" + start + ", end=" + end);
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

    private void recordTask(TaskResult record) {
        taskHistory.addFirst(record);
        while (taskHistory.size() > MAX_TASK_HISTORY) {
            taskHistory.pollLast();
        }
    }

    // ---------------------------------------------------------------- reachability

    private List<WorkerClient> getReachableWorkerServices() {
        Map<Integer, WorkerInfo> activeWorkers = new HashMap<>();
        try {
            for (WorkerInfo worker : bootstrap().getActiveWorkers(EMPTY).getWorkersList()) {
                activeWorkers.put(worker.getWorkerId(), worker);
            }
        } catch (RuntimeException e) {
            System.err.println("[Worker " + workerId + "] could not read active workers: " + e.getMessage());
        }
        activeWorkers.putIfAbsent(workerId, selfInfo);

        List<Integer> reachableWorkerIds = new ArrayList<>();
        Set<Integer> visited = new HashSet<>();
        Queue<Integer> pending = new ArrayDeque<>();
        pending.add(workerId);
        visited.add(workerId);

        while (!pending.isEmpty()) {
            int nextWorkerId = pending.remove();
            WorkerClient client = lookupReachableWorker(activeWorkers, nextWorkerId);
            if (client == null) {
                continue;
            }

            reachableWorkerIds.add(nextWorkerId);
            try {
                for (WorkerInfo neighbour : client.neighbours()) {
                    int neighbourId = neighbour.getWorkerId();
                    if (activeWorkers.containsKey(neighbourId) && visited.add(neighbourId)) {
                        pending.add(neighbourId);
                    }
                }
            } catch (RuntimeException e) {
                System.err.println("[Worker " + workerId + "] Could not inspect neighbours for worker "
                        + nextWorkerId + ": " + e.getMessage());
            }
        }

        Collections.sort(reachableWorkerIds);
        List<WorkerClient> clients = new ArrayList<>();
        Map<Integer, Integer> jacByWorker = new HashMap<>();
        for (Integer reachableWorkerId : reachableWorkerIds) {
            WorkerClient client = lookupReachableWorker(activeWorkers, reachableWorkerId);
            if (client != null) {
                clients.add(client);
                jacByWorker.put(reachableWorkerId, readJAC(client));
            }
        }
        // Phase 2 targeting: lowest-JAC workers first; ties prefer the higher id.
        clients.sort(Comparator
                .comparingInt((WorkerClient worker) -> jacByWorker.getOrDefault(readWorkerId(worker), Integer.MAX_VALUE))
                .thenComparing(Comparator.comparingInt((WorkerClient worker) -> readWorkerId(worker)).reversed()));
        return clients;
    }

    /**
     * The workers a coordinator may hand segments of a client task to: every
     * reachable active worker, including the coordinator itself. This keeps work
     * shared across all active worker nodes when the input is large enough.
     */
    private List<WorkerClient> getAssignmentWorkers() {
        List<WorkerClient> workers = getReachableWorkerServices();
        if (workers.isEmpty()) {
            throw failedPrecondition("No worker nodes reachable for task distribution");
        }
        return workers;
    }

    private WorkerClient lookupReachableWorker(Map<Integer, WorkerInfo> activeWorkers, int targetWorkerId) {
        if (targetWorkerId == workerId) {
            return selfClient;
        }
        WorkerInfo worker = activeWorkers.get(targetWorkerId);
        if (worker == null) {
            return null;
        }
        return remoteClient(worker);
    }

    /**
     * Phase-2 distribution: spread the task across as many reachable worker
     * nodes as the input allows, capped by the active reachable worker count.
     */
    private static int segmentCountFor(int activeWorkers, int totalItems, int perSegment) {
        if (totalItems <= 0) {
            return 0;
        }
        return Math.max(1, Math.min(activeWorkers, totalItems));
    }

    // ---------------------------------------------------------------- local accessors

    /** Rebuilds this worker's neighbour ring and returns the new neighbour count. */
    int syncNeighboursNow() {
        return refreshNeighboursInternal();
    }

    int coordinatorId() {
        return currentCoordinatorId.get();
    }

    List<WorkerInfo> currentNeighbours() {
        return neighbourListInternal();
    }

    String leadermanName() {
        return leaderman;
    }

    /** Releases the executors and gRPC channels held by this worker. */
    public void shutdown() {
        electionScheduler.shutdownNow();
        lotteryScheduler.shutdownNow();
        jobExecutor.shutdownNow();
        channelPool.shutdown();
    }

    @Override
    public String toString() {
        return "WorkerServiceImpl{"
                + "workerId=" + workerId
                + ", priorityId=" + priorityId
                + ", jobAllocationCounter=" + jobAllocationCounter.get()
                + ", neighbours=" + neighbours
                + ", currentCoordinatorId=" + currentCoordinatorId.get()
                + ", leaderman='" + leaderman + '\''
                + '}';
    }
}
