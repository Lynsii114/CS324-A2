package com.cs324.backend.worker;

import com.cs324.common.GrpcSupport;
import com.cs324.common.WorkerClusterConfig;
import com.cs324.grpc.BootstrapServiceGrpc;
import com.cs324.grpc.Empty;
import com.cs324.grpc.IntValue;
import com.cs324.grpc.WorkerHeartbeat;
import com.cs324.grpc.WorkerId;
import com.cs324.grpc.WorkerInfo;
import com.cs324.grpc.WorkerList;
import com.cs324.grpc.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Shared worker-node entry point. Each JVM running this class owns one worker
 * ID, one gRPC port, automatic Bootstrap registration, and heartbeat updates.
 */
public final class WorkerNode {

    public static final String DEFAULT_HOST = "localhost";
    private static final int RPC_DEADLINE_SECONDS = 3;
    private static final long HEARTBEAT_PERIOD_SECONDS = 2;

    private WorkerNode() {
    }

    public static void main(String[] args) {
        WorkerLaunchOptions options = parseOptions(args);
        int workerId = options.workerId() > 0
                ? options.workerId()
                : nextAvailableWorkerId(options.bootstrapHost(), options.bootstrapPort());
        if (workerId < WorkerClusterConfig.FIRST_WORKER_ID) {
            System.err.println("Worker id must be positive, got " + workerId);
            System.exit(1);
        }
        int workerPort = options.workerPort() > 0
                ? options.workerPort()
                : WorkerClusterConfig.BASE_WORKER_PORT + workerId - WorkerClusterConfig.FIRST_WORKER_ID;

        WorkerInfo self = WorkerInfo.newBuilder()
                .setWorkerId(workerId)
                .setHost(options.workerHost())
                .setPort(workerPort)
                .build();

        WorkerServiceImpl worker = new WorkerServiceImpl(
                workerId, self, options.bootstrapHost(), options.bootstrapPort());
        Server server = null;
        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "worker-heartbeat-" + workerId);
            thread.setDaemon(true);
            return thread;
        });
        try {
            server = ServerBuilder.forPort(workerPort).addService(worker).build().start();

            worker.bootstrap().registerWorker(self);
            heartbeat.scheduleWithFixedDelay(
                    () -> sendHeartbeat(worker, self), 0, HEARTBEAT_PERIOD_SECONDS, TimeUnit.SECONDS);
            worker.syncNeighboursNow();
            worker.startAutoElectionChecks();

            final Server boundServer = server;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                heartbeat.shutdownNow();
                unregisterQuietly(worker, workerId);
                worker.shutdown();
                if (boundServer != null) {
                    boundServer.shutdownNow();
                }
            }, "worker-shutdown-" + workerId));

            System.out.println("Worker node " + workerId + " RUNNING on port " + workerPort);

            server.awaitTermination();
        } catch (Exception e) {
            System.err.println("Worker Node failed to start: " + e.getMessage());
            e.printStackTrace();
            if (server != null) {
                server.shutdownNow();
            }
            heartbeat.shutdownNow();
            worker.shutdown();
            System.exit(1);
        }
    }

    private static WorkerLaunchOptions parseOptions(String[] args) {
        if (args.length > 0 && isInteger(args[0]) && (args.length < 2 || !isInteger(args[1]))) {
            usageAndExit();
        }
        if (args.length >= 2 && isInteger(args[0]) && isInteger(args[1])) {
            String bootstrapHost = args.length > 2 ? args[2] : DEFAULT_HOST;
            int bootstrapPort = args.length > 3 ? Integer.parseInt(args[3]) : WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT;
            String workerHost = args.length > 4 ? args[4] : DEFAULT_HOST;
            return new WorkerLaunchOptions(
                    Integer.parseInt(args[0]), Integer.parseInt(args[1]), bootstrapHost, bootstrapPort, workerHost);
        }

        int offset = args.length > 0 && "auto".equalsIgnoreCase(args[0]) ? 1 : 0;
        if (args.length - offset > 3) {
            usageAndExit();
        }
        String bootstrapHost = args.length > offset ? args[offset] : DEFAULT_HOST;
        int bootstrapPort = args.length > offset + 1
                ? Integer.parseInt(args[offset + 1])
                : WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT;
        String workerHost = args.length > offset + 2 ? args[offset + 2] : DEFAULT_HOST;
        return new WorkerLaunchOptions(0, 0, bootstrapHost, bootstrapPort, workerHost);
    }

    private static void usageAndExit() {
        System.err.println("Usage:");
        System.err.println("  WorkerNode [auto] [bootstrapHost] [bootstrapPort] [workerHost]");
        System.err.println("  WorkerNode <workerId> <workerPort> [bootstrapHost] [bootstrapPort] [workerHost]");
        System.exit(1);
    }

    private static boolean isInteger(String value) {
        try {
            Integer.parseInt(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static int nextAvailableWorkerId(String bootstrapHost, int bootstrapPort) {
        Set<Integer> activeIds = activeWorkerIds(bootstrapHost, bootstrapPort);
        for (int workerId = WorkerClusterConfig.FIRST_WORKER_ID; ; workerId++) {
            int port = WorkerClusterConfig.BASE_WORKER_PORT + workerId - WorkerClusterConfig.FIRST_WORKER_ID;
            if (!activeIds.contains(workerId) && !workerAlreadyServing(workerId, port)) {
                System.out.println("Auto-selected Worker node " + workerId + " (port " + port + ")");
                return workerId;
            }
        }
    }

    private static Set<Integer> activeWorkerIds(String bootstrapHost, int bootstrapPort) {
        ManagedChannel channel = null;
        try {
            channel = GrpcSupport.channel(bootstrapHost, bootstrapPort);
            BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap =
                    BootstrapServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
            WorkerList workers = bootstrap.getActiveWorkers(Empty.getDefaultInstance());
            Set<Integer> ids = new HashSet<>();
            for (WorkerInfo worker : workers.getWorkersList()) {
                ids.add(worker.getWorkerId());
            }
            return ids;
        } catch (Exception e) {
            System.err.println("Bootstrap Node not reachable at " + bootstrapHost + ":" + bootstrapPort
                    + ". Start Bootstrap before starting workers.");
            System.exit(1);
            return Set.of();
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    private static void sendHeartbeat(WorkerServiceImpl worker, WorkerInfo self) {
        try {
            worker.bootstrap()
                    .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS)
                    .workerHeartbeat(WorkerHeartbeat.newBuilder().setWorkerId(self.getWorkerId()).build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                try {
                    worker.bootstrap()
                            .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS)
                            .registerWorker(self);
                } catch (RuntimeException ignored) {
                    // Bootstrap may be temporarily unreachable; next heartbeat retries.
                }
            }
        } catch (RuntimeException ignored) {
            // Bootstrap may be temporarily unreachable; next heartbeat retries.
        }
    }

    private static boolean workerAlreadyServing(int workerId, int port) {
        ManagedChannel channel = null;
        try {
            channel = GrpcSupport.channel(DEFAULT_HOST, port);
            WorkerServiceGrpc.WorkerServiceBlockingStub worker = WorkerServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(RPC_DEADLINE_SECONDS, TimeUnit.SECONDS);
            IntValue id = worker.getWorkerId(Empty.getDefaultInstance());
            return id.getValue() == workerId;
        } catch (Exception e) {
            return false;
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
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

    private record WorkerLaunchOptions(
            int workerId,
            int workerPort,
            String bootstrapHost,
            int bootstrapPort,
            String workerHost) {
    }
}
