package com.cs324.backend.bootstrap;

import com.cs324.common.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerClusterLauncher;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.List;

/**
 * Terminal-only entry point that runs the whole cluster from one command.
 * <ol>
 *   <li>Starts a gRPC server and binds the {@link BootstrapServiceImpl}.</li>
 *   <li>Launches every worker node at the same time as its own JVM process
 *       ({@link WorkerClusterLauncher}), so the cluster self-starts and then
 *       elects a coordinator automatically - no GUI is needed.</li>
 * </ol>
 *
 * Usage:
 * <pre>
 *   BootstrapServer [bootstrapPort] [workerCount]
 * </pre>
 * {@code workerCount} is optional and defaults to 6; the minimum is 6 and extra
 * nodes can be requested on top of the base six.
 */
public class BootstrapServer {

    public static void main(String[] args) {
        int port = WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            // Set the property BEFORE WorkerClusterConfig is first loaded so the
            // bootstrap and every spawned worker agree on the cluster size.
            System.setProperty(WorkerClusterConfig.WORKER_COUNT_PROPERTY, args[1]);
        }

        Server server = null;
        List<Integer> launched = List.of();
        try {
            server = ServerBuilder.forPort(port).addService(new BootstrapServiceImpl()).build().start();
            System.out.println("Bootstrap Node gRPC server started on port " + port);
            System.out.println("Serving grpc://<host>:" + port + " (service cs324.BootstrapService)");
            System.out.println("Cluster size: " + WorkerClusterConfig.WORKER_COUNT
                    + " worker node(s) (minimum " + WorkerClusterConfig.MIN_WORKER_COUNT + ")");

            final Server boundServer = server;
            launched = WorkerClusterLauncher.start(WorkerClusterConfig.DEFAULT_HOST, port);
            System.out.println("Bootstrap and " + launched.size() + "/" + WorkerClusterConfig.WORKER_COUNT
                    + " worker node(s) running - wait for the automatic leader election, "
                    + "then submit jobs from the client GUI.");

            // Keep this JVM alive (the gRPC server already does) and clean up the
            // worker processes this session spawned when the bootstrap is stopped.
            final List<Integer> sessionWorkers = launched;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nBootstrap stopping - shutting down the " + sessionWorkers.size()
                        + " worker node(s) started this session...");
                WorkerClusterLauncher.stop(sessionWorkers);
                if (boundServer != null) {
                    boundServer.shutdownNow();
                }
            }, "bootstrap-shutdown"));

            System.out.println("Press Ctrl+C to stop the whole cluster.");
            server.awaitTermination();
        } catch (Exception e) {
            System.err.println("Bootstrap Node failed to start: " + e.getMessage());
            e.printStackTrace();
            if (server != null) {
                server.shutdownNow();
            }
        }
    }
}
