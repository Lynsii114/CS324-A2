package com.cs324.backend.bootstrap;

import com.cs324.common.WorkerClusterConfig;
import com.cs324.grpc.BootstrapServiceGrpc;
import com.cs324.grpc.ClientInfo;
import com.cs324.grpc.ClientList;
import com.cs324.grpc.ClusterStatus;
import com.cs324.grpc.Empty;
import com.cs324.grpc.IntValue;
import com.cs324.grpc.LongValue;
import com.cs324.grpc.PriorityTable;
import com.cs324.grpc.StringValue;
import com.cs324.grpc.WorkerHeartbeat;
import com.cs324.grpc.WorkerId;
import com.cs324.grpc.WorkerInfo;
import com.cs324.grpc.WorkerList;
import com.cs324.grpc.WorkerStatus;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Thread-safe implementation of the Bootstrap Node gRPC service. Keeps
 * membership only and sequences the startup lottery claims (first claim ->
 * priority N); no leader election or job dispatch logic lives here by design.
 */
public class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    private static final long HEARTBEAT_TIMEOUT_MILLIS = 7_500;
    private static final long HEARTBEAT_SCAN_MILLIS = 2_000;

    // ConcurrentHashMap gives us thread-safe register/unregister/read without external locking.
    private final Map<Integer, WorkerInfo> activeWorkers = new ConcurrentHashMap<>();
    private final Map<Integer, Long> workerHeartbeats = new ConcurrentHashMap<>();
    private final Map<String, ClientInfo> activeClients = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatMonitor =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "bootstrap-heartbeat-monitor");
                thread.setDaemon(true);
                return thread;
            });

    /** workerId -> startup priority id (N..1 by lottery claim order). */
    private final Map<Integer, Integer> priorityTable = new LinkedHashMap<>();

    /** Optional legacy cluster size; dynamic workers do not require it. */
    private volatile int configuredWorkerCount = WorkerClusterConfig.WORKER_COUNT;

    /** Set when all configured workers have registered; shared lottery reference. */
    private volatile long clusterCompleteAt = 0L;

    public BootstrapServiceImpl() {
        heartbeatMonitor.scheduleWithFixedDelay(
                this::removeExpiredWorkers, HEARTBEAT_SCAN_MILLIS, HEARTBEAT_SCAN_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void registerWorker(WorkerInfo worker, StreamObserver<Empty> observer) {
        if (worker == null || worker.getWorkerId() <= 0) {
            observer.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("worker must not be null and needs a positive id")
                    .asRuntimeException());
            return;
        }
        WorkerInfo existing = activeWorkers.get(worker.getWorkerId());
        if (existing != null && existing.getPort() != worker.getPort()) {
            observer.onError(io.grpc.Status.ALREADY_EXISTS
                    .withDescription("worker id " + worker.getWorkerId()
                            + " is already registered at " + existing.getHost() + ":" + existing.getPort())
                    .asRuntimeException());
            return;
        }
        activeWorkers.put(worker.getWorkerId(), worker);
        workerHeartbeats.put(worker.getWorkerId(), System.currentTimeMillis());
        System.out.println("[Bootstrap] WORKER NODE REGISTERED: worker "
                + worker.getWorkerId() + " RUNNING at "
                + worker.getHost() + ":" + worker.getPort());
        if (clusterCompleteAt == 0L) {
            clusterCompleteAt = System.currentTimeMillis();
        }
        printStatus("Worker " + worker.getWorkerId() + " connected");
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public synchronized void unregisterWorker(WorkerId request, StreamObserver<Empty> observer) {
        WorkerInfo removed = activeWorkers.remove(request.getWorkerId());
        workerHeartbeats.remove(request.getWorkerId());
        priorityTable.remove(request.getWorkerId());
        if (removed != null) {
            System.out.println("[Bootstrap] Unregistered worker: " + removed);
        } else {
            System.out.println("[Bootstrap] Unregister requested for unknown worker id: " + request.getWorkerId());
        }
        if (activeWorkers.isEmpty()) {
            // Full wipe -> the next cold start performs a fresh lottery.
            priorityTable.clear();
            clusterCompleteAt = 0L;
            System.out.println("[Bootstrap] Cluster empty - lottery state reset");
        }
        printStatus("Worker list updated");
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public synchronized void workerHeartbeat(WorkerHeartbeat request, StreamObserver<Empty> observer) {
        int workerId = request.getWorkerId();
        if (!activeWorkers.containsKey(workerId)) {
            observer.onError(io.grpc.Status.NOT_FOUND
                    .withDescription("worker " + workerId + " is not registered")
                    .asRuntimeException());
            return;
        }
        workerHeartbeats.put(workerId, System.currentTimeMillis());
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public synchronized void getActiveWorkers(Empty request, StreamObserver<WorkerList> observer) {
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        workers.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
        observer.onNext(WorkerList.newBuilder().addAllWorkers(workers).build());
        observer.onCompleted();
    }

    @Override
    public synchronized void getClusterStatus(Empty request, StreamObserver<ClusterStatus> observer) {
        observer.onNext(clusterStatus());
        observer.onCompleted();
    }

    @Override
    public synchronized void getRandomWorker(Empty request, StreamObserver<WorkerInfo> observer) {
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        WorkerInfo chosen = workers.isEmpty()
                ? WorkerInfo.getDefaultInstance()
                : workers.get(ThreadLocalRandom.current().nextInt(workers.size()));
        observer.onNext(chosen);
        observer.onCompleted();
    }

    @Override
    public synchronized void lotteryClaim(WorkerId request, StreamObserver<IntValue> observer) {
        int workerId = request.getWorkerId();
        Integer existing = priorityTable.get(workerId);
        if (existing != null) {
            observer.onNext(IntValue.newBuilder().setValue(existing).build());
            observer.onCompleted();
            return;
        }
        if (priorityTable.size() >= configuredWorkerCount) {
            observer.onNext(IntValue.newBuilder().setValue(-1).build());
            observer.onCompleted();
            return;
        }
        int clusterSize = Math.max(activeWorkers.size(), 1);
        int priority = Math.max(1, clusterSize - priorityTable.size()); // N, N-1, ..., 1
        priorityTable.put(workerId, priority);
        System.out.println("[Bootstrap] Lottery claim by worker " + workerId
                + " -> startup priority " + priority);
        observer.onNext(IntValue.newBuilder().setValue(priority).build());
        observer.onCompleted();
    }

    @Override
    public synchronized void getWorkerPriority(WorkerId request, StreamObserver<IntValue> observer) {
        observer.onNext(IntValue.newBuilder()
                .setValue(priorityTable.getOrDefault(request.getWorkerId(), 0)).build());
        observer.onCompleted();
    }

    @Override
    public synchronized void getPriorityTable(Empty request, StreamObserver<PriorityTable> observer) {
        observer.onNext(PriorityTable.newBuilder().putAllPriorities(new LinkedHashMap<>(priorityTable)).build());
        observer.onCompleted();
    }

    @Override
    public void getClusterCompleteTime(Empty request, StreamObserver<LongValue> observer) {
        observer.onNext(LongValue.newBuilder().setValue(clusterCompleteAt).build());
        observer.onCompleted();
    }

    @Override
    public void getClusterSize(Empty request, StreamObserver<IntValue> observer) {
        int size = configuredWorkerCount > 0 ? configuredWorkerCount : activeWorkers.size();
        observer.onNext(IntValue.newBuilder().setValue(size).build());
        observer.onCompleted();
    }

    @Override
    public synchronized void setClusterSize(IntValue request, StreamObserver<IntValue> observer) {
        int requested = request.getValue();
        if (requested < WorkerClusterConfig.MIN_WORKER_COUNT) {
            observer.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("worker count must be at least " + WorkerClusterConfig.MIN_WORKER_COUNT)
                    .asRuntimeException());
            return;
        }
        if (!activeWorkers.isEmpty() && requested < activeWorkers.size()) {
            observer.onError(io.grpc.Status.FAILED_PRECONDITION
                    .withDescription("cannot set worker count to " + requested
                            + " while " + activeWorkers.size() + " worker(s) are already registered")
                    .asRuntimeException());
            return;
        }
        configuredWorkerCount = requested;
        priorityTable.clear();
        clusterCompleteAt = activeWorkers.size() == configuredWorkerCount ? System.currentTimeMillis() : 0L;
        System.out.println("[Bootstrap] Confirmed cluster size: " + configuredWorkerCount + " worker node(s)");
        printStatus("Worker count confirmed");
        observer.onNext(IntValue.newBuilder().setValue(configuredWorkerCount).build());
        observer.onCompleted();
    }

    @Override
    public synchronized void registerClient(ClientInfo client, StreamObserver<Empty> observer) {
        if (client == null || client.getClientId().isBlank()) {
            observer.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("client must include a clientId")
                    .asRuntimeException());
            return;
        }
        activeClients.put(client.getClientId(), client);
        System.out.println("[Bootstrap] Registered client: " + client.getName()
                + " (" + client.getClientId() + ") from " + client.getHost()
                + " - active clients: " + activeClients.size());
        printStatus("Client " + client.getName() + " connected");
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public synchronized void unregisterClient(StringValue request, StreamObserver<Empty> observer) {
        ClientInfo removed = activeClients.remove(request.getValue());
        if (removed != null) {
            System.out.println("[Bootstrap] Unregistered client: " + removed.getName()
                    + " (" + removed.getClientId() + ") - active clients: " + activeClients.size());
        } else {
            System.out.println("[Bootstrap] Unregister requested for unknown client id: " + request.getValue());
        }
        printStatus("Client list updated");
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public void getActiveClients(Empty request, StreamObserver<ClientList> observer) {
        observer.onNext(ClientList.newBuilder().addAllClients(activeClients.values()).build());
        observer.onCompleted();
    }

    public synchronized void printStatus(String reason) {
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        workers.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
        List<ClientInfo> clients = new ArrayList<>(activeClients.values());
        clients.sort(Comparator.comparing(ClientInfo::getName));

        System.out.println();
        System.out.println("[Bootstrap Status] " + reason);
        System.out.println("  State: RUNNING");
        System.out.println("  Workers: " + workers.size() + " active"
                + (configuredWorkerCount > 0 ? " / " + configuredWorkerCount + " configured" : ""));
        System.out.println("  Registered workers:");
        System.out.println(formatWorkers(workers));
        System.out.println("  Clients: " + clients.size()
                + (clients.isEmpty() ? " - waiting for clients" : " - connected"));
        System.out.println("    " + formatClients(clients));
        System.out.println();
    }

    public void shutdown() {
        heartbeatMonitor.shutdownNow();
    }

    private synchronized void removeExpiredWorkers() {
        long cutoff = System.currentTimeMillis() - HEARTBEAT_TIMEOUT_MILLIS;
        List<Integer> expired = new ArrayList<>();
        for (Map.Entry<Integer, Long> entry : workerHeartbeats.entrySet()) {
            if (entry.getValue() < cutoff) {
                expired.add(entry.getKey());
            }
        }
        if (expired.isEmpty()) {
            return;
        }
        for (Integer workerId : expired) {
            WorkerInfo removed = activeWorkers.remove(workerId);
            workerHeartbeats.remove(workerId);
            priorityTable.remove(workerId);
            if (removed != null) {
                System.out.println("[Bootstrap] Worker " + workerId
                        + " timed out - removed from active registry");
            }
        }
        if (activeWorkers.isEmpty()) {
            priorityTable.clear();
            clusterCompleteAt = 0L;
        }
        printStatus("Heartbeat monitor removed stale worker(s) " + expired);
    }

    private ClusterStatus clusterStatus() {
        int coordinatorId = detectCoordinatorId();
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        workers.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
        ClusterStatus.Builder status = ClusterStatus.newBuilder().setCoordinatorId(coordinatorId);
        for (WorkerInfo worker : workers) {
            status.addWorkers(WorkerStatus.newBuilder()
                    .setWorkerId(worker.getWorkerId())
                    .setHost(worker.getHost())
                    .setPort(worker.getPort())
                    .setStatus("RUNNING")
                    .setLastHeartbeatMillis(workerHeartbeats.getOrDefault(worker.getWorkerId(), 0L))
                    .setCoordinator(worker.getWorkerId() == coordinatorId)
                    .build());
        }
        return status.build();
    }

    private int detectCoordinatorId() {
        for (WorkerInfo worker : activeWorkers.values()) {
            try {
                io.grpc.ManagedChannel channel =
                        com.cs324.common.GrpcSupport.channel(worker.getHost(), worker.getPort());
                try {
                    int coordinatorId = com.cs324.grpc.WorkerServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(1, TimeUnit.SECONDS)
                            .getCurrentCoordinatorId(Empty.getDefaultInstance())
                            .getValue();
                    if (coordinatorId != WorkerClusterConfig.NO_COORDINATOR
                            && activeWorkers.containsKey(coordinatorId)) {
                        return coordinatorId;
                    }
                } finally {
                    channel.shutdownNow();
                }
            } catch (Exception ignored) {
                // Stale workers are removed by the heartbeat monitor.
            }
        }
        return WorkerClusterConfig.NO_COORDINATOR;
    }

    private static String formatWorkers(List<WorkerInfo> workers) {
        if (workers.isEmpty()) {
            return "    none";
        }
        List<String> labels = new ArrayList<>();
        for (WorkerInfo worker : workers) {
            labels.add("    worker " + worker.getWorkerId() + " RUNNING ("
                    + worker.getHost() + ":" + worker.getPort() + ")");
        }
        return String.join(System.lineSeparator(), labels);
    }

    private static String formatClients(List<ClientInfo> clients) {
        if (clients.isEmpty()) {
            return "none";
        }
        List<String> labels = new ArrayList<>();
        for (ClientInfo client : clients) {
            labels.add(client.getName() + " (" + client.getClientId() + ")");
        }
        return String.join(", ", labels);
    }
}
