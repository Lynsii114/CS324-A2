package com.cs324.backend.bootstrap;

import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerClusterLauncher;

import java.rmi.server.ExportException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;

/**
 * Terminal-only entry point that runs the whole cluster from one command.
 * <ol>
 *   <li>Starts (or reuses) an RMI registry and binds the BootstrapService.</li>
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
 * nodes can be requested on top of the base six (the value is clamped with
 * {@link WorkerClusterConfig#MIN_WORKER_COUNT}).
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
            // Set the property BEFORE WorkerClusterConfig is first loaded so the
            // bootstrap and every spawned worker agree on the cluster size.
            System.setProperty(WorkerClusterConfig.WORKER_COUNT_PROPERTY, args[1]);
        }

        try {
            Registry registry;
            try {
                registry = LocateRegistry.createRegistry(port);
                System.out.println("Created RMI registry on port " + port);
            } catch (ExportException alreadyRunning) {
                registry = LocateRegistry.getRegistry(port);
                System.out.println("Using existing RMI registry on port " + port);
            }

            BootstrapServiceImpl service = new BootstrapServiceImpl();
            registry.rebind(SERVICE_NAME, service);

            System.out.println("Bootstrap Node started on port " + port);
            System.out.println("Bound as rmi://<host>:" + port + "/" + SERVICE_NAME);
            System.out.println("Cluster size: " + WorkerClusterConfig.WORKER_COUNT
                    + " worker node(s) (minimum " + WorkerClusterConfig.MIN_WORKER_COUNT + ")");

            List<Integer> launched = WorkerClusterLauncher.start(WorkerClusterConfig.DEFAULT_HOST, port);
            System.out.println("Bootstrap and " + launched.size() + "/" + WorkerClusterConfig.WORKER_COUNT
                    + " worker node(s) running - wait for the automatic leader election, "
                    + "then submit jobs from the client GUI.");

            // Keep this JVM alive (the RMI registry already does) and clean up the
            // worker processes this session spawned when the bootstrap is stopped.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nBootstrap stopping - shutting down the " + launched.size()
                        + " worker node(s) started this session...");
                WorkerClusterLauncher.stop(launched);
            }, "bootstrap-shutdown"));

            System.out.println("Press Ctrl+C to stop the whole cluster.");
        } catch (Exception e) {
            System.err.println("Bootstrap Node failed to start: " + e.getMessage());
            e.printStackTrace();
        }
    }
}