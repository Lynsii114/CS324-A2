package com.cs324.backend.bootstrap;

import com.cs324.common.ClusterConfig;
import com.cs324.common.GrpcChannels;
import com.cs324.proto.ActiveTasksResponse;
import com.cs324.proto.ActiveWorkersResponse;
import com.cs324.proto.BootstrapServiceGrpc;
import com.cs324.proto.ClusterCompleteTimeResponse;
import com.cs324.proto.ClusterSizeResponse;
import com.cs324.proto.Empty;
import com.cs324.proto.LotteryClaimResponse;
import com.cs324.proto.PriorityTableResponse;
import com.cs324.proto.SegmentRecord;
import com.cs324.proto.SegmentUpdate;
import com.cs324.proto.TaskLedgerEntry;
import com.cs324.proto.TaskQuery;
import com.cs324.proto.TaskStatus;
import com.cs324.proto.WorkerIdRequest;
import com.cs324.proto.WorkerInfo;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * gRPC implementation of the Bootstrap Node. It only tracks membership,
 * sequences the startup lottery claims (first claim -> priority N), maintains a
 * heartbeat watchdog over the workers and hosts the replicated task ledger used
 * for coordinator state reconstruction. It never takes part in leader election
 * or job processing.
 */
public class BootstrapServiceImpl extends BootstrapServiceGrpc.BootstrapServiceImplBase {

    private final Map<Integer, WorkerInfo> activeWorkers = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastSeenAt = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> priorityTable = new LinkedHashMap<>();
    private final Map<String, TaskLedgerEntry> taskLedger = new ConcurrentHashMap<>();

    private volatile long clusterCompleteAt = 0L;

    @Override
    public void registerWorker(WorkerInfo request, StreamObserver<Empty> responseObserver) {
        if (request == null || request.getWorkerId() < 1) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("worker info must carry a positive worker id").asException());
            return;
        }
        synchronized (this) {
            activeWorkers.put(request.getWorkerId(), request);
            lastSeenAt.put(request.getWorkerId(), System.currentTimeMillis());
            if (clusterCompleteAt == 0L && activeWorkers.size() >= ClusterConfig.WORKER_COUNT) {
                clusterCompleteAt = System.currentTimeMillis();
                System.out.println("[Bootstrap] Cluster complete: " + ClusterConfig.WORKER_COUNT
                        + "/" + ClusterConfig.WORKER_COUNT + " workers registered");
            }
        }
        System.out.println("[Bootstrap] Registered worker: " + describe(request));
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void unregisterWorker(WorkerIdRequest request, StreamObserver<Empty> responseObserver) {
        int workerId = request.getWorkerId();
        WorkerInfo removed = activeWorkers.remove(workerId);
        lastSeenAt.remove(workerId);
        if (removed != null) {
            System.out.println("[Bootstrap] Unregistered worker: " + describe(removed));
        } else {
            System.out.println("[Bootstrap] Unregister requested for unknown worker id: " + workerId);
        }
        synchronized (this) {
            if (activeWorkers.isEmpty()) {
                priorityTable.clear();
                clusterCompleteAt = 0L;
                System.out.println("[Bootstrap] Cluster empty - lottery state reset");
            }
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void heartbeat(WorkerIdRequest request, StreamObserver<Empty> responseObserver) {
        lastSeenAt.put(request.getWorkerId(), System.currentTimeMillis());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getActiveWorkers(Empty request, StreamObserver<ActiveWorkersResponse> responseObserver) {
        List<WorkerInfo> sorted = new ArrayList<>(activeWorkers.values());
        sorted.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
        responseObserver.onNext(ActiveWorkersResponse.newBuilder().addAllWorkers(sorted).build());
        responseObserver.onCompleted();
    }

    /** Latest snapshot of a single worker, incl. its live heartbeat flag. */
    public WorkerInfo workerInfoOr(int workerId, WorkerInfo fallback) {
        WorkerInfo info = activeWorkers.get(workerId);
        if (info == null) {
            return fallback;
        }
        return withOnline(info);
    }

    @Override
    public void getRandomWorker(Empty request, StreamObserver<WorkerInfo> responseObserver) {
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        if (workers.isEmpty()) {
            responseObserver.onNext(WorkerInfo.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }
        WorkerInfo picked = workers.get(ThreadLocalRandom.current().nextInt(workers.size()));
        responseObserver.onNext(withOnline(picked));
        responseObserver.onCompleted();
    }

    @Override
    public void lotteryClaim(WorkerIdRequest request, StreamObserver<LotteryClaimResponse> responseObserver) {
        int workerId = request.getWorkerId();
        int priority;
        synchronized (this) {
            Integer existing = priorityTable.get(workerId);
            if (existing != null) {
                priority = existing;
            } else if (priorityTable.size() >= ClusterConfig.WORKER_COUNT) {
                priority = -1;
            } else {
                priority = ClusterConfig.WORKER_COUNT - priorityTable.size(); // N, N-1, ..., 1
                priorityTable.put(workerId, priority);
                System.out.println("[Bootstrap] Lottery claim by worker " + workerId
                        + " -> startup priority " + priority
                        + " (claims so far: " + priorityTable.size() + "/" + ClusterConfig.WORKER_COUNT + ")");
            }
        }
        responseObserver.onNext(LotteryClaimResponse.newBuilder().setPriority(priority).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getWorkerPriority(WorkerIdRequest request, StreamObserver<LotteryClaimResponse> responseObserver) {
        int priority = priorityTable.getOrDefault(request.getWorkerId(), 0);
        responseObserver.onNext(LotteryClaimResponse.newBuilder().setPriority(priority).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getPriorityTable(Empty request, StreamObserver<PriorityTableResponse> responseObserver) {
        PriorityTableResponse.Builder builder = PriorityTableResponse.newBuilder();
        synchronized (this) {
            builder.putAllPriorities(priorityTable);
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void getClusterCompleteTime(Empty request, StreamObserver<ClusterCompleteTimeResponse> responseObserver) {
        responseObserver.onNext(ClusterCompleteTimeResponse.newBuilder().setEpochMillis(clusterCompleteAt).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getClusterSize(Empty request, StreamObserver<ClusterSizeResponse> responseObserver) {
        responseObserver.onNext(ClusterSizeResponse.newBuilder().setSize(ClusterConfig.WORKER_COUNT).build());
        responseObserver.onCompleted();
    }

    // ------------------------------------------------------------------ ledger

    @Override
    public void recordTaskStart(TaskLedgerEntry request, StreamObserver<Empty> responseObserver) {
        if (request.getTaskId().isBlank()) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("task id must not be blank").asException());
            return;
        }
        taskLedger.put(request.getTaskId(), request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void recordSegmentUpdate(SegmentUpdate request, StreamObserver<Empty> responseObserver) {
        synchronized (this) {
            TaskLedgerEntry existing = taskLedger.get(request.getTaskId());
            if (existing == null) {
                responseObserver.onNext(Empty.getDefaultInstance());
                responseObserver.onCompleted();
                return;
            }
            SegmentRecord record = SegmentRecord.newBuilder()
                    .setSegmentIndex(request.getSegmentIndex())
                    .setWorkerId(request.getWorkerId())
                    .setCompleted(request.getCompleted())
                    .setPartial(request.getPartial())
                    .setFailed(request.getFailed())
                    .build();
            TaskLedgerEntry updated = existing.toBuilder().putSegments(request.getSegmentIndex(), record).build();
            taskLedger.put(updated.getTaskId(), updated);
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void recordTaskCompletion(TaskLedgerEntry request, StreamObserver<Empty> responseObserver) {
        taskLedger.put(request.getTaskId(), request.toBuilder().setStatus(TaskStatus.TASK_DONE).build());
        System.out.println("[Bootstrap] Ledger: task " + shortId(request.getTaskId()) + " DONE (client "
                + request.getClientId() + ", result=" + request.getFinalResult() + ")");
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void recordTaskFailure(TaskLedgerEntry request, StreamObserver<Empty> responseObserver) {
        taskLedger.put(request.getTaskId(), request.toBuilder().setStatus(TaskStatus.TASK_FAILED).build());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getTaskLedger(TaskQuery request, StreamObserver<TaskLedgerEntry> responseObserver) {
        TaskLedgerEntry entry = taskLedger.get(request.getTaskId());
        responseObserver.onNext(entry == null ? TaskLedgerEntry.getDefaultInstance() : entry);
        responseObserver.onCompleted();
    }

    @Override
    public void listActiveTasks(Empty request, StreamObserver<ActiveTasksResponse> responseObserver) {
        ActiveTasksResponse.Builder builder = ActiveTasksResponse.newBuilder();
        for (TaskLedgerEntry entry : taskLedger.values()) {
            if (entry.getStatus() == TaskStatus.TASK_RUNNING
                    || entry.getStatus() == TaskStatus.TASK_RESUMED) {
                builder.addEntries(entry);
            }
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /** True when the worker's last heartbeat is within the freshness window. */
    public boolean isOnline(int workerId) {
        Long last = lastSeenAt.get(workerId);
        if (last == null) {
            return false;
        }
        return System.currentTimeMillis() - last < ClusterConfig.HEARTBEAT_TIMEOUT_MS;
    }

    public List<WorkerInfo> onlineWorkers() {
        List<WorkerInfo> result = new ArrayList<>();
        for (WorkerInfo worker : activeWorkers.values()) {
            result.add(withOnline(worker));
        }
        result.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
        return result;
    }

    private WorkerInfo withOnline(WorkerInfo info) {
        return info.toBuilder().setOnline(isOnline(info.getWorkerId())).build();
    }

    private static String describe(WorkerInfo info) {
        return "WorkerInfo{workerId=" + info.getWorkerId() + ", host='" + info.getHost()
                + "', port=" + info.getPort() + "}";
    }

    private static String shortId(String taskId) {
        return taskId != null && taskId.length() > 8 ? taskId.substring(0, 8) : taskId;
    }

    /** Close all cached client channels when the bootstrap process exits. */
    public static void shutdown() {
        GrpcChannels.shutdownAll();
    }
}