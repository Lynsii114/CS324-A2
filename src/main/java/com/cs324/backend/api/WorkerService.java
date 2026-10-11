package com.cs324.backend.api;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

/**
 * RMI-exposed contract for a Worker Node. Exposes worker state, neighbour
 * management and the distributed leader election protocol.
 */
public interface WorkerService extends Remote {

    /** Sentinel used when no coordinator has been elected yet. */
    int NO_COORDINATOR = -1;

    /** A coordinator may process at most this many submitted jobs during one term. */
    int COORDINATOR_TERM_LIMIT = 5;

    int getWorkerId() throws RemoteException;

    /** Startup-lottery priority id (1..N, N = configured worker count), {@code 0} until the lottery minted it. */
    int getPriorityId() throws RemoteException;

    int getJobAllocationCounter() throws RemoteException;

    /** Increases the Job Acceptance Counter (JAC) of this worker by one. */
    int incrementJobAllocationCounter() throws RemoteException;

    /**
     * Records one job allocation made by this worker while it is coordinator.
     * Used when the coordinator delegates a section of a distributed job to
     * another worker.
     */
    int recordJobAllocation() throws RemoteException;

    int resetJobAllocationCounter() throws RemoteException;

    /**
     * How many submitted jobs this worker has handled as coordinator in the
     * current term (isolated from the JAC). Reset to zero at the start of each
     * new term in which this worker is elected coordinator.
     */
    int getJobsThisTerm() throws RemoteException;

    List<WorkerInfo> getNeighbours() throws RemoteException;

    void addNeighbour(WorkerInfo worker) throws RemoteException;

    /** Adds a neighbour by id, resolving its host/port from the Bootstrap Node. */
    void addNeighbour(int workerId) throws RemoteException;

    void removeNeighbour(int workerId) throws RemoteException;

    /** Current coordinator id, or {@link #NO_COORDINATOR} if none is present. */
    int getCurrentCoordinatorId() throws RemoteException;

    void setCurrentCoordinatorId(int coordinatorId) throws RemoteException;

    String getLeaderman() throws RemoteException;

    /**
     * Rebuilds the neighbour ring from the Bootstrap Node's registry of active
     * workers. Returns the new number of neighbours.
     */
    int syncNeighbours() throws RemoteException;

    /**
     * Starts a distributed leader election. Only proceeds if no coordinator is
     * present; otherwise returns the current coordinator.
     */
    String initiateElection() throws RemoteException;

    /**
     * Handles an incoming ELECTION message. Returns {@code null} if the election
     * was already processed (deduplication), otherwise the merged participant set.
     */
    ProcessedParticipants receiveElection(ElectionMessage message) throws RemoteException;

    /** Records the elected coordinator for a finished election. */
    void announceWinner(WinnerAnnouncement announcement) throws RemoteException;

    /**
     * Distributes a MAX job across reachable workers and returns the compiled
     * result to the client. {@code clientId} identifies the submitting client;
     * the coordinator generates a unique task id and records which workers
     * processed each segment in the returned {@link TaskResult}.
     */
    TaskResult submitMaxJob(String clientId, List<Integer> numbers) throws RemoteException;

    int computePartialMax(List<Integer> numbers) throws RemoteException;

    /**
     * Distributes a PRIMECOUNT job across reachable workers. Coordinator only.
     * Returns a {@link TaskResult} with the number of primes (duplicates count)
     * and the workers that handled the segments.
     */
    TaskResult submitPrimeCount(String clientId, List<Integer> numbers) throws RemoteException;

    /** Computes the number of primes in a section assigned by the coordinator. */
    int countPrimes(List<Integer> numbers) throws RemoteException;

    /**
     * Distributes a PRIMESUM job across reachable workers. Coordinator only.
     * Returns a {@link TaskResult} with the sum of all prime numbers in the
     * inclusive range {@code [start, end]} and the workers used per segment.
     */
    TaskResult submitPrimeSum(String clientId, int start, int end) throws RemoteException;

    /** Computes the sum of the primes in the inclusive range assigned by the coordinator. */
    long sumPrimeRange(int start, int end) throws RemoteException;

    /**
     * Recent distributed-task records this worker produced while acting as the
     * coordinator (newest first). Used to show which
     * worker nodes processed the tasks of different clients.
     */
    List<TaskResult> getTaskHistory() throws RemoteException;

    /**
     * Term-End notification from a stepping-down coordinator, carrying its final
     * verified JAC table so every worker can log the workload snapshot that
     * drives the lowest-JAC re-election.
     */
    void notifyTermEnd(int priorCoordinatorId, Map<Integer, Integer> finalJacTable) throws RemoteException;
}