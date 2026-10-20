package com.cs324.frontend.client;

import com.cs324.common.ClusterConfig;
import com.cs324.common.GrpcChannels;
import com.cs324.proto.BootstrapServiceGrpc;
import com.cs324.proto.Empty;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Opens several client windows at once, so you can demonstrate that a single
 * cluster serves multiple concurrent clients: each window gets its own client
 * id, its own task list and its own job stream, while all of them share the
 * elected coordinator.
 *
 * <p>The client is a GUI helper, not a cluster component: nothing here talks to
 * the workers directly, it only spawns {@link ClientGUI} processes (each of
 * which speaks gRPC to the bootstrap and the coordinator).</p>
 *
 * Usage:
 * <pre>
 *   ClientLauncher [count] [bootstrapHost] [bootstrapPort]
 * </pre>
 * {@code count} defaults to 2 and is capped at {@link #MAX_CLIENTS} (the GUI
 * needs a window per client, so this is deliberately modest).
 */
public final class ClientLauncher {

    /** How many client windows this launcher will open at most. */
    public static final int MAX_CLIENTS = 8;

    private ClientLauncher() {
    }

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 2;
        String bootstrapHost = args.length > 1 ? args[1] : ClusterConfig.DEFAULT_HOST;
        int bootstrapPort = args.length > 2 ? Integer.parseInt(args[2]) : EndpointDefaults.BOOTSTRAP_PORT;

        if (count < 1) {
            System.err.println("count must be at least 1");
            System.exit(1);
        }
        if (count > MAX_CLIENTS) {
            System.out.println("Capping " + count + " clients at " + MAX_CLIENTS);
            count = MAX_CLIENTS;
        }

        checkBootstrap(bootstrapHost, bootstrapPort);

        String classpath = currentClasspath();
        String javaBin = Paths.get(System.getProperty("java.home"), "bin", "java").toString();

        List<Process> started = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            ProcessBuilder builder = new ProcessBuilder(
                    javaBin,
                    "-cp", classpath,
                    ClientGUI.class.getName(),
                    String.valueOf(bootstrapPort),
                    "Client " + i);
            builder.directory(Paths.get("").toAbsolutePath().toFile());
            Process process = builder.start();
            started.add(process);
            System.out.println("Started Client " + i + " (pid " + process.pid()
                    + ", bootstrap " + bootstrapHost + ":" + bootstrapPort + ")");
        }

        System.out.println();
        System.out.println(started.size() + " client window(s) launched - press Connect in each of them.");
        System.out.println("Each window has its own client id, so their jobs run concurrently on the "
                + "cluster and are tracked separately.");
    }

    /** Warns early (rather than on every submit) when the Bootstrap Node is not up yet. */
    private static void checkBootstrap(String host, int port) {
        try {
            BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap = GrpcChannels.bootstrap(host, port);
            int workers = bootstrap.getClusterSize(Empty.getDefaultInstance()).getSize();
            System.out.println("Bootstrap Node reachable at " + host + ":" + port
                    + " (cluster configured for " + workers + " worker node(s))");
        } catch (Exception e) {
            System.err.println("WARNING: Bootstrap Node not reachable at " + host + ":" + port
                    + " - start it first (bootstrap-server module). The clients will still open,") ;
            System.err.println("         but Connect will keep reporting a failure until it is up.");
        }
        GrpcChannels.close(host, port);
    }

    /**
     * Resolves the classpath a client JVM needs: this module's classes plus the
     * shared {@code common} classes (so {@code com.cs324.common.*} and the gRPC
     * stubs resolve) and the dependency jars from {@code target/classpath.txt}.
     */
    private static String currentClasspath() {
        String classesDirs = codeSourceLocation();
        try {
            classesDirs = classesDirs + File.pathSeparator + commonClassesDir();
        } catch (Exception ignored) {
            // common module not built next to us; the packaged jar is on the
            // dependency classpath below instead.
        }

        Path classpathFile = Paths.get("target", "classpath.txt");
        if (Files.exists(classpathFile)) {
            try {
                String deps = Files.readString(classpathFile, StandardCharsets.UTF_8).trim();
                if (!deps.isEmpty()) {
                    return classesDirs + File.pathSeparator + deps;
                }
            } catch (IOException ignored) {
                // fall back to classes only
            }
        }
        return classesDirs;
    }

    private static String codeSourceLocation() {
        try {
            URI uri = ClientLauncher.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            return Paths.get(uri).toAbsolutePath().toString();
        } catch (Exception e) {
            return System.getProperty("java.class.path", "");
        }
    }

    private static String commonClassesDir() {
        return Paths.get("..", "common", "target", "classes").toAbsolutePath().normalize().toString();
    }
}
