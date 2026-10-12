package com.cs324.common;

import io.grpc.ManagedChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reuses gRPC channels per {@code host:port}. Channels are relatively heavy to
 * build, so every process keeps one per peer for its lifetime and shuts them
 * all down on exit. This replaces the per-call RMI registry lookups.
 */
public final class ChannelPool {

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();

    public ManagedChannel channel(String host, int port) {
        return channels.computeIfAbsent(host + ":" + port, key -> GrpcSupport.channel(host, port));
    }

    /** Shuts down every channel this pool opened. Safe to call more than once. */
    public void shutdown() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
        channels.clear();
    }
}
