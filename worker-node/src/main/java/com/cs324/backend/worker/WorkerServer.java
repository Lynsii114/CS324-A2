package com.cs324.backend.worker;

import com.cs324.common.GrpcChannels;
import com.cs324.proto.BootstrapServiceGrpc;
import com.cs324.proto.WorkerIdRequest;
import com.cs324.proto.WorkerInfo;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Entry point that runs a Worker Node as its own JVM process. Each worker
 * hosts its own gRPC server on a dedicated port ({@link
 * ClusterConfig#portFor(int)}) serving both its {@code ClusterService}
 * and {@code ClientService}; it registers with the Bootstrap Node, then the
 * startup lottery and leader election run automatically (no GUI needed).
 */
public class WorkerServer {

    public static final String DEFAULT_HOST = "localhost";

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WorkerServer <workerId> <workerPort> [bootstrapHost] [bootstrapPort] [workerHost]");
            System.exit(1);
        }

        int workerId = Integer.parseInt(args[0]);
        int workerPort = Integer.parseInt(args[1]);
        String bootstrapHost = args.length > 2 ? args[2] : DEFAULT_HOST;
        int bootstrapPort = args.length > 3 ? Integer.parseInt(args[3]) : BootstrapEndpoint.DEFAULT_PORT;
        String workerHost = args.length > 4 ? args[4] : DEFAULT_HOST;

        WorkerNode node = new WorkerNode(workerId, workerHost, workerPort, bootstrapHost, bootstrapPort);

        Server server = ServerBuilder.forPort(workerPort)
                .addService(new WorkerClusterService(node))
                .addService(new WorkerClientService(node))
                .build();
        server.start();

        BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap =
                GrpcChannels.bootstrap(bootstrapHost, bootstrapPort);
        bootstrap.registerWorker(WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(workerHost)
                .setPort(workerPort)
                .setOnline(true)
                .build());

        node.startBackgroundServices();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                bootstrap.unregisterWorker(WorkerIdRequest.newBuilder().setWorkerId(workerId).build());
                System.out.println("Worker Node " + workerId + " unregistered from Bootstrap Node");
            } catch (Exception e) {
                System.err.println("Could not unregister worker " + workerId + ": " + e.getMessage());
            }
            server.shutdownNow();
            node.shutdown();
            GrpcChannels.shutdownAll();
        }, "worker-shutdown"));

        System.out.println("Worker Node " + workerId + " started on gRPC port " + workerPort
                + " (ClusterService + ClientService)");
        System.out.println("Registered with Bootstrap Node at " + bootstrapHost + ":" + bootstrapPort);
        System.out.println("Initial state: JAC=0, priority=PENDING (startup lottery), coordinatorId="
                + node.getCurrentCoordinatorId() + " (none), neighbours=" + node.getNeighbours().size());

        server.awaitTermination();
    }
}