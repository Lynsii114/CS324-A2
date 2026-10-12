package com.cs324.backend.worker;

import com.cs324.common.WorkerClusterConfig;
import com.cs324.grpc.WorkerId;
import com.cs324.grpc.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Entry point that runs a Worker Node as its own JVM process. It starts a gRPC
 * server on the worker's own port and registers the node with the Bootstrap
 * Node over gRPC.
 */
public class WorkerServer {

    public static final String DEFAULT_HOST = "localhost";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: WorkerServer <workerId> <workerPort> [bootstrapHost] [bootstrapPort] [workerHost]");
            System.exit(1);
        }

        int workerId = Integer.parseInt(args[0]);
        int workerPort = Integer.parseInt(args[1]);
        String bootstrapHost = args.length > 2 ? args[2] : DEFAULT_HOST;
        int bootstrapPort = args.length > 3 ? Integer.parseInt(args[3]) : WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT;
        String workerHost = args.length > 4 ? args[4] : DEFAULT_HOST;

        WorkerInfo self = WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(workerHost)
                .setPort(workerPort)
                .build();

        WorkerServiceImpl worker = new WorkerServiceImpl(workerId, self, bootstrapHost, bootstrapPort);
        Server server = null;
        try {
            server = ServerBuilder.forPort(workerPort).addService(worker).build().start();
            System.out.println("Worker gRPC server listening on port " + workerPort);

            worker.bootstrap().registerWorker(self);
            worker.syncNeighboursNow();
            worker.startAutoElectionChecks();
            worker.startStartupLottery();

            final Server boundServer = server;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                unregisterQuietly(worker, workerId);
                worker.shutdown();
                if (boundServer != null) {
                    boundServer.shutdownNow();
                }
            }, "worker-shutdown-" + workerId));

            System.out.println("Worker Node " + workerId + " started on port " + workerPort);
            System.out.println("Serving grpc://" + workerHost + ":" + workerPort + " (service cs324.WorkerService)");
            System.out.println("Registered with Bootstrap Node at " + bootstrapHost + ":" + bootstrapPort);
            System.out.println("Initial state: JAC=0, priority=PENDING (startup lottery), coordinatorId="
                    + worker.coordinatorId()
                    + " (none), leaderman=" + worker.leadermanName()
                    + ", neighbours=" + worker.currentNeighbours());

            server.awaitTermination();
        } catch (Exception e) {
            System.err.println("Worker Node failed to start: " + e.getMessage());
            e.printStackTrace();
            if (server != null) {
                server.shutdownNow();
            }
            worker.shutdown();
            System.exit(1);
        }
    }

    private static void unregisterQuietly(WorkerServiceImpl worker, int workerId) {
        try {
            worker.bootstrap().unregisterWorker(WorkerId.newBuilder().setWorkerId(workerId).build());
            System.out.println("Worker Node " + workerId + " unregistered from Bootstrap Node");
        } catch (Exception e) {
            System.err.println("Could not unregister worker " + workerId + ": " + e.getMessage());
        }
    }
}
