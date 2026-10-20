package com.cs324.backend.bootstrap;

import com.cs324.common.ClusterConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Terminal entry point for the Bootstrap Node only. It runs the gRPC
 * BootstrapService (membership, startup lottery, task ledger) and nothing
 * else - it deliberately does <b>not</b> spawn worker processes, so every part
 * of the cluster can be started, stopped and observed independently in its own
 * terminal.
 *
 * <p>Typical layout (one terminal each, run from the repository root):</p>
 * <pre>
 *   Terminal 1: (cd bootstrap-server && java -cp "target/classes:$(cat target/classpath.txt)" \
 *                  com.cs324.backend.bootstrap.BootstrapServer [port] [workerCount])
 *   Terminal 2: (cd worker-node && java -cp "target/classes:$(cat target/classpath.txt)" \
 *                  com.cs324.backend.worker.WorkerServer 1 5001)
 *   Terminal 3: (cd worker-node && ... WorkerServer 2 5002)
 *   ...
 *   Terminal N: (cd client && java -cp "target/classes:$(cat target/classpath.txt)" \
 *                  com.cs324.frontend.client.ClientGUI)
 * </pre>
 * (The {@link com.cs324.backend.worker.WorkerClusterLauncher} - a separate
 * module - remains available as a convenience for bulk-starting workers if you
 * prefer fewer terminals.)
 *
 * Usage:
 * <pre>
 *   BootstrapServer [bootstrapPort] [workerCount]
 * </pre>
 * {@code workerCount} is optional and defaults to 6; the minimum is 6 and extra
 * nodes can be requested on top of the base six (the value is clamped with
 * {@link ClusterConfig#MIN_WORKER_COUNT}). The bootstrap uses it to know
 * how many workers to expect before it declares the cluster complete and starts
 * the startup lottery.
 */
public class BootstrapServer {

    public static final String SERVICE_NAME = "BootstrapService";
    public static final int DEFAULT_PORT = 1099;

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }
        if (args.length > 1) {
            // Set the property BEFORE ClusterConfig is first loaded so the
            // bootstrap and every manually-started worker agree on the cluster size.
            System.setProperty(ClusterConfig.WORKER_COUNT_PROPERTY, args[1]);
        }

        try {
            Server server = ServerBuilder.forPort(port)
                    .addService(new BootstrapServiceImpl())
                    .build();
            server.start();

            System.out.println("Bootstrap Node started on gRPC port " + port);
            System.out.println("Expecting " + ClusterConfig.WORKER_COUNT
                    + " worker node(s) (minimum " + ClusterConfig.MIN_WORKER_COUNT + ").");
            System.out.println("Start each worker in its own terminal, e.g.:");
            for (int id : ClusterConfig.workerIds()) {
                System.out.println("  (cd worker-node && java -cp \"target/classes:$(cat target/classpath.txt)\" "
                        + "com.cs324.backend.worker.WorkerServer " + id + " "
                        + ClusterConfig.portFor(id) + ")");
            }
            System.out.println("Press Ctrl+C to stop the Bootstrap Node.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nBootstrap Node stopping...");
                server.shutdownNow();
                BootstrapServiceImpl.shutdown();
            }, "bootstrap-shutdown"));

            server.awaitTermination();
        } catch (Exception e) {
            System.err.println("Bootstrap Node failed to start: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}