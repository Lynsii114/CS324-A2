package com.cs324.backend.worker;

import com.cs324.proto.ClientServiceGrpc;
import com.cs324.proto.HistoryQuery;
import com.cs324.proto.JobRequest;
import com.cs324.proto.TaskHistoryResponse;
import com.cs324.proto.TaskStatusQuery;
import com.cs324.proto.TaskUpdate;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * gRPC {@code ClientService} implementation hosted by every worker JVM, so any
 * node can serve as the coordinator. {@code SubmitJob} is gRPC server
 * streaming: the coordinator pushes {@link TaskUpdate} messages (percent
 * complete plus a per-worker breakdown) while the task runs, and the final
 * update carries the merged result. The two getters back the client GUI's
 * status tracking and history views.
 */
public class WorkerClientService extends ClientServiceGrpc.ClientServiceImplBase {

    private final WorkerNode node;

    public WorkerClientService(WorkerNode node) {
        this.node = node;
    }

    @Override
    public void submitJob(JobRequest request, StreamObserver<TaskUpdate> responseObserver) {
        if (request == null || request.getJobType() == null
                || request.getJobType() == com.cs324.proto.JobType.JOB_TYPE_UNSPECIFIED) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("job request must carry a job type").asException());
            return;
        }
        node.submitJob(request, responseObserver);
    }

    @Override
    public void getTaskStatus(TaskStatusQuery request, StreamObserver<TaskUpdate> responseObserver) {
        try {
            responseObserver.onNext(node.getTaskStatus(request.getTaskId()));
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.fromThrowable(e).asException());
        }
    }

    @Override
    public void getTaskHistory(HistoryQuery request, StreamObserver<TaskHistoryResponse> responseObserver) {
        responseObserver.onNext(TaskHistoryResponse.newBuilder()
                .addAllTasks(node.getTaskHistory()).build());
        responseObserver.onCompleted();
    }
}