package com.cs324.backend.bootstrap;

import com.cs324.common.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerClusterLauncher;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.List;

/**
 * Terminal-only Bootstrap Node entry point.
 * <ol>
 *   <li>Starts a gRPC server and binds the {@link BootstrapServiceImpl}.</li>
 *   <li>By default, does not launch workers. Start worker JVMs manually, or
 *       pass {@code --launch-workers} for the old one-command cluster mode.</li>
 * </ol>
 *
 * Usage:
 * <pre>
 *   BootstrapServer [bootstrapPort] [workerCount] [--launch-workers]
 * </pre>
 * {@code workerCount} is optional and defaults to 0 until a client confirms a
 * cluster size; confirmed clusters must contain 1..10 worker nodes.
 */
public class BootstrapServer {

    public static void main(String[] args) {
        BootstrapOptions options = parseOptions(args);
        if (options.workerCount != null) {
            // Set the property BEFORE WorkerClusterConfig is first loaded so the
            // bootstrap and every spawned worker agree on the cluster size.
            System.setProperty(WorkerClusterConfig.WORKER_COUNT_PROPERTY, options.workerCount);
        }

        Server server = null;
        List<Integer> launched = List.of();
        try {
            BootstrapServiceImpl bootstrapService = new BootstrapServiceImpl();
            server = ServerBuilder.forPort(options.port).addService(bootstrapService).build().start();
            System.out.println("Bootstrap Node gRPC server started on port " + options.port);
            System.out.println("Serving grpc://<host>:" + options.port + " (service cs324.BootstrapService)");
            System.out.println("Cluster size: " + WorkerClusterConfig.WORKER_COUNT
                    + " worker node(s) (minimum " + WorkerClusterConfig.MIN_WORKER_COUNT + ")");
            bootstrapService.printStatus("Bootstrap running");

            final Server boundServer = server;
            if (options.launchWorkers) {
                launched = WorkerClusterLauncher.start(WorkerClusterConfig.DEFAULT_HOST, options.port);
                System.out.println("Bootstrap and " + launched.size() + "/" + WorkerClusterConfig.WORKER_COUNT
                        + " worker node(s) running - wait for the automatic leader election, "
                        + "then submit jobs from the client GUI.");
            } else {
                System.out.println("Manual worker mode: no workers launched by bootstrap.");
                System.out.println("Start workers separately, then connect from the client GUI.");
            }

            // Keep this JVM alive (the gRPC server already does) and clean up the
            // worker processes this session spawned when the bootstrap is stopped.
            final List<Integer> sessionWorkers = launched;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nBootstrap stopping - shutting down the " + sessionWorkers.size()
                        + " worker node(s) started this session...");
                WorkerClusterLauncher.stop(sessionWorkers);
                bootstrapService.shutdown();
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

    private static BootstrapOptions parseOptions(String[] args) {
        int port = 1099;
        String workerCount = null;
        boolean launchWorkers = false;
        int positional = 0;

        for (String arg : args) {
            if ("--launch-workers".equals(arg)) {
                launchWorkers = true;
                continue;
            }
            if ("--no-workers".equals(arg)) {
                launchWorkers = false;
                continue;
            }
            if (positional == 0) {
                port = Integer.parseInt(arg);
            } else if (positional == 1) {
                workerCount = arg;
            } else {
                throw new IllegalArgumentException(
                        "Usage: BootstrapServer [bootstrapPort] [workerCount] [--launch-workers]");
            }
            positional++;
        }

        return new BootstrapOptions(port, workerCount, launchWorkers);
    }

    private record BootstrapOptions(int port, String workerCount, boolean launchWorkers) {
    }
}
