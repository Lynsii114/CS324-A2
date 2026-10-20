package com.cs324.backend.worker;

/**
 * The address of the Bootstrap Node as a worker node knows it. This is the only
 * piece of bootstrap information a worker needs, so the worker project does not
 * link against the bootstrap-server project at all - the two talk purely over
 * the gRPC {@code BootstrapService} contract defined in {@code distrilab.proto}
 * (see the {@code common} module).
 *
 * <p>Keep {@link #DEFAULT_PORT} in step with the Bootstrap Node's own default
 * port; passing {@code [bootstrapHost] [bootstrapPort]} to
 * {@link WorkerServer} / {@link WorkerClusterLauncher} overrides it.</p>
 */
public final class BootstrapEndpoint {

    /** Default host the workers look the Bootstrap Node up on. */
    public static final String DEFAULT_HOST = "localhost";

    /** Default gRPC port the Bootstrap Node listens on (BootstrapServer's own default). */
    public static final int DEFAULT_PORT = 1099;

    private BootstrapEndpoint() {
    }
}
