package com.cs324.backend.coordinator;

import com.cs324.coordinator.grpc.CoordinatorServiceGrpc;
import com.cs324.coordinator.grpc.ProgressReport;
import com.cs324.coordinator.grpc.ProgressSummary;
import com.cs324.coordinator.grpc.RecoveryRequest;
import com.cs324.coordinator.grpc.RegisterRequest;
import com.cs324.coordinator.grpc.RegisterResponse;
import com.cs324.coordinator.grpc.SnapshotState;
import com.cs324.coordinator.grpc.TaskAssignment;
import com.cs324.coordinator.grpc.TaskResult;
import com.cs324.coordinator.grpc.WorkerMetadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/** Coordinator endpoints for worker membership, streamed progress, and recovery snapshots. */
public final class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
    private final ConcurrentMap<String, WorkerMetadata> workers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ProgressReport> progressBySegment = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TaskResult> completedResults = new ConcurrentHashMap<>();
    private final AtomicLong progressSequence = new AtomicLong();

    @Override
    public void registerWorker(RegisterRequest request, StreamObserver<RegisterResponse> observer) {
        if (request.getWorkerIp().isBlank() || request.getWorkerPort() < 1 || request.getWorkerPort() > 65535) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("worker_ip must be set and worker_port must be between 1 and 65535")
                    .asRuntimeException());
            return;
        }

        String workerId = UUID.randomUUID().toString();
        String endpoint = request.getWorkerIp() + ":" + request.getWorkerPort();
        workers.put(workerId, WorkerMetadata.newBuilder()
                .setWorkerId(workerId)
                .setWorkerEndpoint(endpoint)
                .setStatus("ACTIVE")
                .build());
        System.out.printf("[Coordinator] Registered worker %s at %s with capabilities %s%n",
                workerId, endpoint, request.getCapabilitiesList());

        observer.onNext(RegisterResponse.newBuilder()
                .setWorkerId(workerId)
                .setStatus(true)
                .build());
        observer.onCompleted();
    }

    @Override
    public StreamObserver<ProgressReport> streamProgress(StreamObserver<ProgressSummary> responseObserver) {
        return new StreamObserver<>() {
            private boolean failed;

            @Override
            public void onNext(ProgressReport report) {
                if (failed) return;
                if (report.getWorkerId().isBlank() || report.getTaskId().isBlank()
                        || report.getSegmentId() < 0
                        || !Float.isFinite(report.getCompletionPercentage())
                        || report.getCompletionPercentage() < 0.0f
                        || report.getCompletionPercentage() > 100.0f) {
                    failed = true;
                    responseObserver.onError(Status.INVALID_ARGUMENT
                            .withDescription("progress requires worker/task IDs, a nonnegative segment, and completion in [0,100]")
                            .asRuntimeException());
                    return;
                }

                String key = report.getTaskId() + "/" + report.getSegmentId();
                progressBySegment.compute(key, (ignored, previous) -> {
                    if (previous == null || report.getCompletionPercentage() >= previous.getCompletionPercentage()) {
                        return report;
                    }
                    return previous;
                });
                progressSequence.incrementAndGet();
                List<ProgressReport> taskSegments = progressBySegment.values().stream()
                        .filter(update -> update.getTaskId().equals(report.getTaskId()))
                        .toList();
                double aggregateCompletion = taskSegments.stream()
                        .mapToDouble(ProgressReport::getCompletionPercentage).average().orElse(0.0);
                long aggregateRows = taskSegments.stream().mapToLong(ProgressReport::getRowsProcessed).sum();
                System.out.printf("[Coordinator] Progress task=%s segment=%d worker=%s completion=%.1f%% rows=%d status=%s%n",
                        report.getTaskId(), report.getSegmentId(), report.getWorkerId(),
                        report.getCompletionPercentage(), report.getRowsProcessed(), report.getStatusMessage());
                System.out.printf("[Coordinator] Aggregate task=%s segments=%d completion=%.1f%% rows=%d%n",
                        report.getTaskId(), taskSegments.size(), aggregateCompletion, aggregateRows);

                if ("COMPLETED".equalsIgnoreCase(report.getStatusMessage())) {
                    completedResults.putIfAbsent(report.getTaskId(), TaskResult.newBuilder()
                            .setTaskId(report.getTaskId())
                            .setStatus("COMPLETED")
                            .setOutputLocation("memory://results/" + report.getTaskId())
                            .setExecutionTimeMs(0)
                            .build());
                }
            }

            @Override
            public void onError(Throwable error) {
                System.err.println("[Coordinator] Progress stream failed: " + error.getMessage());
            }

            @Override
            public void onCompleted() {
                if (!failed) {
                    responseObserver.onNext(ProgressSummary.newBuilder().setAcknowledged(true).build());
                    responseObserver.onCompleted();
                }
            }
        };
    }

    @Override
    public void recoverState(RecoveryRequest request, StreamObserver<SnapshotState> observer) {
        List<WorkerMetadata> activeWorkers = new ArrayList<>(workers.values());
        activeWorkers.sort(Comparator.comparing(WorkerMetadata::getWorkerId));

        SnapshotState.Builder snapshot = SnapshotState.newBuilder()
                .setSnapshotTimestamp(System.currentTimeMillis())
                .addAllActiveWorkers(activeWorkers);

        // Progress is keyed by task/segment, so group it to reconstruct assignments.
        Map<String, List<ProgressReport>> byTask = new ConcurrentHashMap<>();
        for (ProgressReport progress : progressBySegment.values()) {
            byTask.computeIfAbsent(progress.getTaskId(), ignored -> new ArrayList<>()).add(progress);
        }
        byTask.forEach((taskId, reports) -> {
            reports.sort(Comparator.comparingInt(ProgressReport::getSegmentId));
            List<Integer> segments = reports.stream().map(ProgressReport::getSegmentId).toList();
            String workerId = reports.get(0).getWorkerId();
            boolean allComplete = reports.stream().allMatch(r -> r.getCompletionPercentage() >= 100.0f);
            if (allComplete) return;
            snapshot.addOngoingAssignments(TaskAssignment.newBuilder()
                    .setTaskId(taskId)
                    .setAssignedWorkerId(workerId)
                    .setTotalSegments(segments.size())
                    .addAllAssignedSegments(segments)
                    .setAssignedAtTimestamp(0L));
        });
        snapshot.addAllCompletedResults(completedResults.values());

        System.out.printf("[Coordinator] Recovery requested by %s after sequence %d; current sequence=%d%n",
                request.getCoordinatorId(), request.getLastKnownSequence(), progressSequence.get());
        observer.onNext(snapshot.build());
        observer.onCompleted();
    }
}
