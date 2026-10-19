package com.cs324.backend.worker;

import com.cs324.proto.CoordinatorIdResponse;
import com.cs324.proto.ElectionMessage;
import com.cs324.proto.ElectionResult;
import com.cs324.proto.Empty;
import com.cs324.proto.JacResponse;
import com.cs324.proto.NeighboursResponse;
import com.cs324.proto.NumbersRequest;
import com.cs324.proto.ProcessedParticipants;
import com.cs324.proto.RangeRequest;
import com.cs324.proto.SectionResult;
import com.cs324.proto.SetCoordinatorRequest;
import com.cs324.proto.SyncNeighboursResponse;
import com.cs324.proto.ClusterServiceGrpc;
import com.cs324.proto.TermEndNotification;
import com.cs324.proto.WinnerAnnouncement;
import com.cs324.proto.WorkerIdRequest;
import com.cs324.proto.WorkerInfo;
import com.cs324.proto.WorkerState;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * gRPC {@code ClusterService} implementation hosted by every worker JVM. It is
 * the worker-to-worker contract: identity/state inspection (used by the
 * coordinator liveness monitor and by other workers for load assignment),
 * neighbour-ring management, leader election message handling, term handling
 * and the segment computation RPCs that a coordinator delegates to workers.
 */
public class WorkerClusterService extends ClusterServiceGrpc.ClusterServiceImplBase {

    private final WorkerNode node;

    public WorkerClusterService(WorkerNode node) {
        this.node = node;
    }

    @Override
    public void getWorkerState(Empty request, StreamObserver<WorkerState> responseObserver) {
        responseObserver.onNext(node.getWorkerState());
        responseObserver.onCompleted();
    }

    @Override
    public void incrementJac(Empty request, StreamObserver<JacResponse> responseObserver) {
        responseObserver.onNext(JacResponse.newBuilder().setValue(node.incrementJobAllocationCounter()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void recordAllocation(Empty request, StreamObserver<JacResponse> responseObserver) {
        responseObserver.onNext(JacResponse.newBuilder().setValue(node.recordAllocation()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void resetJac(Empty request, StreamObserver<JacResponse> responseObserver) {
        responseObserver.onNext(JacResponse.newBuilder().setValue(node.resetJobAllocationCounter()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void getNeighbours(Empty request, StreamObserver<NeighboursResponse> responseObserver) {
        responseObserver.onNext(NeighboursResponse.newBuilder()
                .addAllNeighbours(node.getNeighbours()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void addNeighbour(WorkerInfo request, StreamObserver<Empty> responseObserver) {
        node.addNeighbour(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeNeighbour(WorkerIdRequest request, StreamObserver<Empty> responseObserver) {
        node.removeNeighbour(request.getWorkerId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void syncNeighbours(Empty request, StreamObserver<SyncNeighboursResponse> responseObserver) {
        int count = node.syncNeighbours();
        responseObserver.onNext(SyncNeighboursResponse.newBuilder().setCount(count).build());
        responseObserver.onCompleted();
    }

    @Override
    public void setCurrentCoordinator(SetCoordinatorRequest request, StreamObserver<Empty> responseObserver) {
        node.setCurrentCoordinatorId(request.getCoordinatorId());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getCurrentCoordinatorId(Empty request, StreamObserver<CoordinatorIdResponse> responseObserver) {
        responseObserver.onNext(CoordinatorIdResponse.newBuilder()
                .setValue(node.getCurrentCoordinatorId()).build());
        responseObserver.onCompleted();
    }

    @Override
    public void initiateElection(Empty request, StreamObserver<ElectionResult> responseObserver) {
        String message = node.initiateElection();
        responseObserver.onNext(ElectionResult.newBuilder()
                .setMessage(message)
                .setCoordinatorId(node.getCurrentCoordinatorId())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void receiveElection(ElectionMessage request, StreamObserver<ProcessedParticipants> responseObserver) {
        ProcessedParticipants reply = node.receiveElection(request);
        if (reply == null) {
            responseObserver.onError(Status.ABORTED
                    .withDescription("duplicate or malformed election message").asException());
            return;
        }
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    @Override
    public void announceWinner(WinnerAnnouncement request, StreamObserver<Empty> responseObserver) {
        node.announceWinner(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void notifyTermEnd(TermEndNotification request, StreamObserver<Empty> responseObserver) {
        node.notifyTermEnd(request);
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void computePartialMax(NumbersRequest request, StreamObserver<SectionResult> responseObserver) {
        responseObserver.onNext(SectionResult.newBuilder()
                .setValue(node.computePartialMax(request.getNumbersList())).build());
        responseObserver.onCompleted();
    }

    @Override
    public void countPrimes(NumbersRequest request, StreamObserver<SectionResult> responseObserver) {
        responseObserver.onNext(SectionResult.newBuilder()
                .setValue(node.countPrimes(request.getNumbersList())).build());
        responseObserver.onCompleted();
    }

    @Override
    public void sumPrimeRange(RangeRequest request, StreamObserver<SectionResult> responseObserver) {
        responseObserver.onNext(SectionResult.newBuilder()
                .setValue(node.sumPrimeRange(request.getStart(), request.getEnd())).build());
        responseObserver.onCompleted();
    }
}