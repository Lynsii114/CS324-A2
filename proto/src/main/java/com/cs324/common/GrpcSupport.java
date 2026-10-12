package com.cs324.common;

import com.cs324.grpc.BootstrapServiceGrpc;
import com.cs324.grpc.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * Small helper around gRPC channel/stub creation so the bootstrap, worker and
 * client projects all dial each other the same way. This is the gRPC
 * replacement for the {@code LocateRegistry} + {@code lookup} pattern that RMI
 * used.
 */
public final class GrpcSupport {

    private GrpcSupport() {
    }

    /** Opens a plaintext channel to a host/port. Callers that cache channels must shut them down. */
    public static ManagedChannel channel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    public static BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrapStub(String host, int port) {
        return BootstrapServiceGrpc.newBlockingStub(channel(host, port));
    }

    public static BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrapStub(ManagedChannel channel) {
        return BootstrapServiceGrpc.newBlockingStub(channel);
    }

    public static WorkerServiceGrpc.WorkerServiceBlockingStub workerStub(String host, int port) {
        return WorkerServiceGrpc.newBlockingStub(channel(host, port));
    }

    public static WorkerServiceGrpc.WorkerServiceBlockingStub workerStub(ManagedChannel channel) {
        return WorkerServiceGrpc.newBlockingStub(channel);
    }
}
