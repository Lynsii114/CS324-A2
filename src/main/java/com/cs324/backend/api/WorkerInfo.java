package com.cs324.backend.api;

import java.io.Serializable;
import java.util.Objects;

/**
 * Plain data holder describing a worker that has registered with the Bootstrap Node.
 * Sent over RMI, so it must be Serializable.
 */
public class WorkerInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int workerId;
    private final String host;
    private final int port;

    public WorkerInfo(int workerId, String host, int port) {
        this.workerId = workerId;
        this.host = host;
        this.port = port;
    }

    public int getWorkerId() {
        return workerId;
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
        if (!(o instanceof WorkerInfo)) return false;
        WorkerInfo other = (WorkerInfo) o;
        return workerId == other.workerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerId);
    }

    @Override
    public String toString() {
        return "WorkerInfo{workerId=" + workerId + ", host='" + host + "', port=" + port + "}";
    }
}
