package com.cs324.backend.worker;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.WorkerInfo;
import com.cs324.backend.bootstrap.BootstrapServer;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;

/**
 * Entry point that runs a Worker Node as its own JVM process.
 */
public class WorkerServer {

    public static final String SERVICE_NAME_PREFIX = "WorkerService-";
    public static final String DEFAULT_HOST = "localhost";

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: WorkerServer <workerId> <workerPort> [bootstrapHost] [bootstrapPort] [workerHost]");
            System.exit(1);
        }

        int workerId = Integer.parseInt(args[0]);
        int workerPort = Integer.parseInt(args[1]);
        String bootstrapHost = args.length > 2 ? args[2] : DEFAULT_HOST;
        int bootstrapPort = args.length > 3 ? Integer.parseInt(args[3]) : BootstrapServer.DEFAULT_PORT;
        String workerHost = args.length > 4 ? args[4] : DEFAULT_HOST;
        String serviceName = SERVICE_NAME_PREFIX + workerId;

        try {
            Registry workerRegistry = getOrCreateRegistry(workerPort);
            WorkerInfo self = new WorkerInfo(workerId, workerHost, workerPort);

            Registry bootstrapRegistry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            BootstrapService bootstrap = (BootstrapService) bootstrapRegistry.lookup(BootstrapServer.SERVICE_NAME);

            WorkerServiceImpl worker = new WorkerServiceImpl(workerId, self, bootstrap);
            workerRegistry.rebind(serviceName, worker);

            bootstrap.registerWorker(self);
            worker.syncNeighbours();
            worker.startAutoElectionChecks();
            worker.startStartupLottery();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> unregisterQuietly(bootstrap, workerId)));

            System.out.println("Worker Node " + workerId + " started on port " + workerPort);
            System.out.println("Bound as rmi://" + workerHost + ":" + workerPort + "/" + serviceName);
            System.out.println("Registered with Bootstrap Node at " + bootstrapHost + ":" + bootstrapPort);
            System.out.println("Initial state: JAC=0, priority=PENDING (startup lottery), coordinatorId="
                    + worker.getCurrentCoordinatorId()
                    + " (none), leaderman=" + worker.getLeaderman()
                    + ", neighbours=" + worker.getNeighbours());
        } catch (Exception e) {
            System.err.println("Worker Node failed to start: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Registry getOrCreateRegistry(int port) throws Exception {
        try {
            Registry registry = LocateRegistry.createRegistry(port);
            System.out.println("Created worker RMI registry on port " + port);
            return registry;
        } catch (ExportException alreadyRunning) {
            System.out.println("Using existing worker RMI registry on port " + port);
            return LocateRegistry.getRegistry(port);
        }
    }

    private static void unregisterQuietly(BootstrapService bootstrap, int workerId) {
        try {
            bootstrap.unregisterWorker(workerId);
            System.out.println("Worker Node " + workerId + " unregistered from Bootstrap Node");
        } catch (Exception e) {
            System.err.println("Could not unregister worker " + workerId + ": " + e.getMessage());
        }
    }
}
