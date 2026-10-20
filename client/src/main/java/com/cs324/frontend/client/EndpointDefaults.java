package com.cs324.frontend.client;

/**
 * Defaults the standalone client needs in order to talk to a cluster it was not
 * built with: the Bootstrap Node's gRPC port and the sentinel it displays when
 * no worker currently holds the coordinator role.
 *
 * <p>The client project therefore links only against the shared gRPC contract
 * ({@code common}) - it never depends on the bootstrap-server or worker-node
 * projects. Every default can be overridden at runtime from the GUI
 * ({@code [bootstrapPort]} argument or the dashboard's port field).</p>
 */
public final class EndpointDefaults {

    /** Default gRPC port of the Bootstrap Node (matches BootstrapServer's own default). */
    public static final int BOOTSTRAP_PORT = 1099;

    /** Coordinator id meaning "no coordinator elected / known yet". */
    public static final int NO_COORDINATOR = -1;

    private EndpointDefaults() {
    }
}
