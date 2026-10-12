package com.cs324.backend.bootstrap;

import com.cs324.common.WorkerClusterConfig;
import com.cs324.grpc.BootstrapServiceGrpc;
import com.cs324.grpc.Empty;
import com.cs324.grpc.IntValue;
import com.cs324.grpc.LongValue;
import com.cs324.grpc.PriorityTable;
import com.cs324.grpc.WorkerId;
import com.cs324.grpc.WorkerInfo;
import com.cs324.grpc.WorkerList;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Thread-safe implementation of the Bootstrap Node gRPC service. Keeps
 * membership only and sequences the startup lottery claims (first claim ->
 * priority N); no leader election or job dispatch logic lives here by design.
 */
public class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    // ConcurrentHashMap gives us thread-safe register/unregister/read without external locking.
    private final Map<Integer, WorkerInfo> activeWorkers = new ConcurrentHashMap<>();

    /** workerId -> startup priority id (N..1 by lottery claim order). */
    private final Map<Integer, Integer> priorityTable = new LinkedHashMap<>();

    /** Set when all WORKER_COUNT workers have registered; shared lottery reference. */
    private volatile long clusterCompleteAt = 0L;

    @Override
    public synchronized void registerWorker(WorkerInfo worker, StreamObserver<Empty> observer) {
        if (worker == null || worker.getWorkerId() <= 0) {
            observer.onError(io.grpc.Status.INVALID_ARGUMENT
                    .withDescription("worker must not be null and needs a positive id")
                    .asRuntimeException());
            return;
        }
        activeWorkers.put(worker.getWorkerId(), worker);
        System.out.println("[Bootstrap] Registered worker: " + worker);
        if (clusterCompleteAt == 0L && activeWorkers.size() == WorkerClusterConfig.WORKER_COUNT) {
            clusterCompleteAt = System.currentTimeMillis();
            System.out.println("[Bootstrap] Cluster complete: " + WorkerClusterConfig.WORKER_COUNT
                    + "/" + WorkerClusterConfig.WORKER_COUNT + " workers registered");
        }
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public synchronized void unregisterWorker(WorkerId request, StreamObserver<Empty> observer) {
        WorkerInfo removed = activeWorkers.remove(request.getWorkerId());
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
        observer.onNext(Empty.getDefaultInstance());
        observer.onCompleted();
    }

    @Override
    public void getActiveWorkers(Empty request, StreamObserver<WorkerList> observer) {
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        observer.onNext(WorkerList.newBuilder().addAllWorkers(workers).build());
        observer.onCompleted();
    }

    @Override
    public void getRandomWorker(Empty request, StreamObserver<WorkerInfo> observer) {
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
        if (priorityTable.size() >= WorkerClusterConfig.WORKER_COUNT) {
            observer.onNext(IntValue.newBuilder().setValue(-1).build());
            observer.onCompleted();
            return;
        }
        int priority = WorkerClusterConfig.WORKER_COUNT - priorityTable.size(); // N, N-1, ..., 1
        priorityTable.put(workerId, priority);
        System.out.println("[Bootstrap] Lottery claim by worker " + workerId
                + " -> startup priority " + priority
                + " (claims so far: " + priorityTable.size() + "/" + WorkerClusterConfig.WORKER_COUNT + ")");
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
        observer.onNext(IntValue.newBuilder().setValue(WorkerClusterConfig.WORKER_COUNT).build());
        observer.onCompleted();
    }
}
