package com.cs324.backend.api;

import java.io.Serializable;
import java.util.Objects;

/**
 * Snapshot of a worker (workerId, JAC, priority id, host, port) taken at the
 * moment an election runs. Sent over RMI inside {@link ElectionMessage}, so it
 * must be Serializable.
 */
public final class CandidateInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final int jac;
    private final int priorityId;
    private final String host;
    private final int port;

    public CandidateInfo(int workerId, int jac, String host, int port) {
        this(workerId, jac, 0, host, port);
    }

    public CandidateInfo(int workerId, int jac, int priorityId, String host, int port) {
        this.workerId = workerId;
        this.jac = jac;
        this.priorityId = priorityId;
        this.host = host;
        this.port = port;
    }

    public int getWorkerId() {
        return workerId;
    }

    public int getJac() {
        return jac;
    }

    /** Startup-lottery priority id (1..N, N = configured worker count); used as the election tie-breaker. */
    public int getPriorityId() {
        return priorityId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CandidateInfo)) return false;
        CandidateInfo other = (CandidateInfo) o;
        return workerId == other.workerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerId);
    }

    @Override
    public String toString() {
        return "CandidateInfo{workerId=" + workerId + ", jac=" + jac + ", priorityId=" + priorityId
                + ", host='" + host + "', port=" + port + "}";
    }
}