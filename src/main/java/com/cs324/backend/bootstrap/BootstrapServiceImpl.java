package com.cs324.backend.bootstrap;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.WorkerInfo;
import com.cs324.backend.worker.WorkerClusterConfig;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Thread-safe implementation of the Bootstrap Node. Keeps membership only and
 * sequences the startup lottery claims (first claim -> priority 6); no leader
 * election or job dispatch logic lives here by design.
 */
public class BootstrapServiceImpl extends UnicastRemoteObject implements BootstrapService {
    private static final long serialVersionUID = 1L;

    // ConcurrentHashMap gives us thread-safe register/unregister/read without external locking.
    private final Map<Integer, WorkerInfo> activeWorkers = new ConcurrentHashMap<>();

    /** workerId -> startup priority id (6..1 by lottery claim order). */
    private final Map<Integer, Integer> priorityTable = new LinkedHashMap<>();

    /** Set when all WORKER_COUNT workers have registered; shared lottery reference. */
    private volatile long clusterCompleteAt = 0L;

    public BootstrapServiceImpl() throws RemoteException {
        super();
    }

    @Override
    public synchronized void registerWorker(WorkerInfo worker) throws RemoteException {
        if (worker == null) {
            throw new IllegalArgumentException("worker must not be null");
        }
        activeWorkers.put(worker.getWorkerId(), worker);
        System.out.println("[Bootstrap] Registered worker: " + worker);
        if (clusterCompleteAt == 0L && activeWorkers.size() == WorkerClusterConfig.WORKER_COUNT) {
            clusterCompleteAt = System.currentTimeMillis();
            System.out.println("[Bootstrap] Cluster complete: " + WorkerClusterConfig.WORKER_COUNT
                    + "/" + WorkerClusterConfig.WORKER_COUNT + " workers registered");
        }
    }

    @Override
    public synchronized void unregisterWorker(int workerId) throws RemoteException {
        WorkerInfo removed = activeWorkers.remove(workerId);
        if (removed != null) {
            System.out.println("[Bootstrap] Unregistered worker: " + removed);
        } else {
            System.out.println("[Bootstrap] Unregister requested for unknown worker id: " + workerId);
        }
        if (activeWorkers.isEmpty()) {
            // Full wipe -> the next cold start performs a fresh lottery.
            priorityTable.clear();
            clusterCompleteAt = 0L;
            System.out.println("[Bootstrap] Cluster empty - lottery state reset");
        }
    }

    @Override
    public List<WorkerInfo> getActiveWorkers() throws RemoteException {
        return new ArrayList<>(activeWorkers.values());
    }

    @Override
    public WorkerInfo getRandomWorker() throws RemoteException {
        List<WorkerInfo> workers = new ArrayList<>(activeWorkers.values());
        if (workers.isEmpty()) {
            return null;
        }
        int index = ThreadLocalRandom.current().nextInt(workers.size());
        return workers.get(index);
    }

    @Override
    public synchronized int lotteryClaim(int workerId) throws RemoteException {
        Integer existing = priorityTable.get(workerId);
        if (existing != null) {
            return existing;
        }
        if (priorityTable.size() >= WorkerClusterConfig.WORKER_COUNT) {
            return -1;
        }
        int priority = WorkerClusterConfig.WORKER_COUNT - priorityTable.size(); // 6, 5, ..., 1
        priorityTable.put(workerId, priority);
        System.out.println("[Bootstrap] Lottery claim by worker " + workerId
                + " -> startup priority " + priority
                + " (claims so far: " + priorityTable.size() + "/" + WorkerClusterConfig.WORKER_COUNT + ")");
        return priority;
    }

    @Override
    public int getWorkerPriority(int workerId) throws RemoteException {
        return priorityTable.getOrDefault(workerId, 0);
    }

    @Override
    public synchronized Map<Integer, Integer> getPriorityTable() throws RemoteException {
        return new HashMap<>(priorityTable);
    }

    @Override
    public long getClusterCompleteTime() throws RemoteException {
        return clusterCompleteAt;
    }

    @Override
    public int getClusterSize() throws RemoteException {
        return WorkerClusterConfig.WORKER_COUNT;
    }
}
