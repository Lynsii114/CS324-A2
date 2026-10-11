package com.cs324.backend.api;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * RMI-exposed contract for the Bootstrap Node. It only tracks membership and
 * sequences the startup lottery claims - it never takes part in leader election
 * or job processing.
 */
public interface BootstrapService extends Remote {

    void registerWorker(WorkerInfo worker) throws RemoteException;

    void unregisterWorker(int workerId) throws RemoteException;

    List<WorkerInfo> getActiveWorkers() throws RemoteException;

    WorkerInfo getRandomWorker() throws RemoteException;

    /**
     * Startup lottery claim: assigns, in order of arrival of the claim, the
     * startup priority ids {@code 6, 5, ..., 1}. The first worker to fire its
     * randomized countdown gets priority 6. Idempotent: a worker that already
     * claimed keeps its priority.
     */
    int lotteryClaim(int workerId) throws RemoteException;

    /** Startup priority id of a worker, or {@code 0} when it has not claimed yet. */
    int getWorkerPriority(int workerId) throws RemoteException;

    /** Full workerId -> priorityId mapping minted by the startup lottery. */
    Map<Integer, Integer> getPriorityTable() throws RemoteException;

    /** Epoch millis at which all {@code WORKER_COUNT} workers had registered, or {@code 0}. */
    long getClusterCompleteTime() throws RemoteException;

    /** Total number of worker nodes this cluster is configured with (minimum 6). */
    int getClusterSize() throws RemoteException;
}
