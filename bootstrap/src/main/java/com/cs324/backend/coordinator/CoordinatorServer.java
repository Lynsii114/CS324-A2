package com.cs324.backend.coordinator;

import io.grpc.Server;
import io.grpc.ServerBuilder;

/** Standalone entry point for the coordinator gRPC service. */
public final class CoordinatorServer {
    private static final int DEFAULT_PORT = 50051;

    private CoordinatorServer() { }

    public static void main(String[] args) {
        int port = args.length == 0 ? DEFAULT_PORT : Integer.parseInt(args[0]);
        Server server = null;
        try {
            server = ServerBuilder.forPort(port)
                    .addService(new CoordinatorServiceImpl())
                    .build()
                    .start();
            System.out.println("Coordinator gRPC server listening on :" + port);

            Server runningServer = server;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Stopping coordinator gRPC server...");
                runningServer.shutdown();
            }, "coordinator-shutdown"));
            server.awaitTermination();
        } catch (Exception e) {
            System.err.println("Coordinator failed to start: " + e.getMessage());
            e.printStackTrace();
            if (server != null) server.shutdownNow();
        }
    }
}
