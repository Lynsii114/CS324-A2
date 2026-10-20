package com.cs324.common;

import com.cs324.proto.BootstrapServiceGrpc;
import com.cs324.proto.ClientServiceGrpc;
import com.cs324.proto.ClusterServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Caches {@link ManagedChannel}s per target host:port so every component
 * (bootstrap, workers, clients, launcher) reuses a single channel per peer.
 * A modest keep-alive is configured but pings are only sent while a call is
 * in flight (never on idle connections): pinging idle, long-lived channels to
 * every peer trips the HTTP/2 ping-rate limiter on the server
 * (GOAWAY / ENHANCE_YOUR_CALM "too_many_pings"). Detection of a dead
 * coordinator during an otherwise-silent stream is instead owned by the
 * client's per-call deadline, which is what drives failover resume.
 */
public final class GrpcChannels {

    private static final ConcurrentHashMap<String, ManagedChannel> CHANNELS = new ConcurrentHashMap<>();

    private static final long KEEPALIVE_SECONDS = 30;

    private GrpcChannels() {
    }

    public static ManagedChannel channel(String host, int port) {
        String key = host + ":" + port;
        ManagedChannel existing = CHANNELS.get(key);
        if (existing != null && !existing.isShutdown()) {
            return existing;
        }
        ManagedChannel created = NettyChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .maxInboundMessageSize(64 * 1024 * 1024)
                .keepAliveTime(KEEPALIVE_SECONDS, TimeUnit.SECONDS)
                .keepAliveTimeout(KEEPALIVE_SECONDS, TimeUnit.SECONDS)
                .build();
        ManagedChannel raced = CHANNELS.putIfAbsent(key, created);
        if (raced != null && !raced.isShutdown()) {
            created.shutdownNow();
            return raced;
        }
        return created;
    }

    public static BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap(String host, int port) {
        return BootstrapServiceGrpc.newBlockingStub(channel(host, port));
    }

    public static ClusterServiceGrpc.ClusterServiceBlockingStub cluster(String host, int port) {
        return ClusterServiceGrpc.newBlockingStub(channel(host, port));
    }

    public static ClientServiceGrpc.ClientServiceBlockingStub clientBlocking(String host, int port) {
        return ClientServiceGrpc.newBlockingStub(channel(host, port));
    }

    public static ClientServiceGrpc.ClientServiceStub clientAsync(String host, int port) {
        return ClientServiceGrpc.newStub(channel(host, port));
    }

    /** Closes every cached channel; used by shutdown hooks. */
    public static void shutdownAll() {
        CHANNELS.forEach((k, ch) -> {
            ch.shutdown();
            try {
                ch.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        CHANNELS.clear();
    }

    public static void close(String host, int port) {
        ManagedChannel removed = CHANNELS.remove(host + ":" + port);
        if (removed != null) {
            removed.shutdown();
        }
    }
}