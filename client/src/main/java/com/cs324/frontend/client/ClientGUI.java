package com.cs324.frontend.client;

import com.cs324.common.ChannelPool;
import com.cs324.common.WorkerClusterConfig;
import com.cs324.frontend.ui.UITheme;
import com.cs324.grpc.BootstrapServiceGrpc;
import com.cs324.grpc.ClientInfo;
import com.cs324.grpc.ClusterStatus;
import com.cs324.grpc.Empty;
import com.cs324.grpc.IntValue;
import com.cs324.grpc.MaxJobRequest;
import com.cs324.grpc.NumberList;
import com.cs324.grpc.PrimeCountRequest;
import com.cs324.grpc.PrimeSumRequest;
import com.cs324.grpc.StringValue;
import com.cs324.grpc.TaskResult;
import com.cs324.grpc.WorkerInfo;
import com.cs324.grpc.WorkerServiceGrpc;
import com.cs324.grpc.WorkerStatus;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.AbstractTableModel;
import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.GraphicsEnvironment;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client GUI. Runs as its own Java process and talks to the coordinator worker
 * over gRPC. Supports manual or CSV input, three job types (MAX, PRIMECOUNT,
 * PRIMESUM) and concurrent submissions from one or more clients at the same
 * time.
 *
 * <p>All gRPC calls happen on a small background executor, never on the Swing
 * event thread, so the interface never freezes while a job runs.</p>
 */
public class ClientGUI extends JFrame {

    private static final Empty EMPTY = Empty.getDefaultInstance();
    private static final String[] JOB_TYPES = {"MAX", "PRIMECOUNT", "PRIMESUM"};
    private static final String[] COLUMNS = {"Job ID", "Term", "Type", "Submitted", "Status", "Coordinator", "Workers", "Result", "Error"};
    private static final long PROBE_TIMEOUT_SECONDS = 2;
    private static final long COORDINATOR_WAIT_MILLIS = 1_500L;
    private static final long COORDINATOR_HANDOFF_WAIT_MILLIS = 30_000L;
    private static final long COORDINATOR_POLL_MILLIS = 75L;

    private final JTextField hostField = new JTextField("localhost", 10);
    private final JTextField bootstrapPortField =
            new JTextField(String.valueOf(WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT), 6);
    private final JButton connectButton = new JButton("Connect");
    private final JButton resetWorkersButton = new JButton("Reset Workers");
    private final JLabel connectionStatus = new JLabel("Not connected");

    private final JComboBox<String> jobTypeCombo = new JComboBox<>(JOB_TYPES);
    private final JTextArea inputArea = new JTextArea(2, 40);
    private final JButton loadCsvButton = new JButton("Load CSV...");
    private final JButton submitButton = new JButton("Submit");
    private final JButton clearButton = new JButton("Clear");
    private final JButton resetJobsButton = new JButton("Reset Jobs");

    // Dashboard widgets.
    private final JLabel statusPill = new JLabel("  O  ");
    private final JLabel coordinatorValue = new JLabel("None");
    private final JLabel workersValue = new JLabel("0 active");
    private final JLabel workerListValue = new JLabel("none");
    private final JLabel jobsSubmittedValue = new JLabel("0");
    private final JLabel completionValue = new JLabel("0%");
    private final JProgressBar completionProgress = new JProgressBar(0, 100);
    private final JLabel coordinatorLabel = new JLabel("Coordinator: none");

    private final JTextArea logArea = new JTextArea(8, 60);
    private final JTable taskTable = new JTable();
    private final TaskTableModel tableModel = new TaskTableModel();

    private final ExecutorService jobExecutor = Executors.newFixedThreadPool(4, runnable -> {
        Thread thread = new Thread(runnable, "client-job-executor");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicInteger jobSequence = new AtomicInteger(0);
    private final Set<Integer> completedJobs = ConcurrentHashMap.newKeySet();
    private final ChannelPool channels = new ChannelPool();
    private final Timer workerRefreshTimer;

    /**
     * This client's identifier. Sent with every submission so the elected
     * coordinator can correlate the distributed task back to the client that
     * asked for it (shown in the client's task table).
     */
    private final String clientName;
    private final String clientId;

    private volatile String bootstrapHost = "localhost";
    private volatile int bootstrapPort = WorkerClusterConfig.DEFAULT_BOOTSTRAP_PORT;
    private volatile BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private volatile WorkerServiceGrpc.WorkerServiceBlockingStub coordinator;
    private volatile int activeWorkerCount;
    private volatile boolean workerRefreshInProgress = false;
    private volatile boolean connectInProgress = false;
    private final List<WorkerInfo> latestWorkers = new ArrayList<>();
    private volatile int latestCoordinatorId = WorkerClusterConfig.NO_COORDINATOR;
    private volatile int latestCoordinatorTermJobs = 0;

    public static void main(String[] args) {
        String requestedName = args.length == 0 ? "" : String.join(" ", args).trim();
        String displayName = requestedName.isEmpty() ? allocateClientName() : requestedName;
        SwingUtilities.invokeLater(() -> new ClientGUI(displayName).setVisible(true));
    }

    public ClientGUI() {
        this(allocateClientName());
    }

    public ClientGUI(String clientName) {
        super("Distributed Client - " + clientName);
        this.clientName = clientName;
        this.clientId = readableClientId(clientName);
        this.workerRefreshTimer = new Timer(1000, e -> refreshWorkerStatusAsync());
        UITheme.install();
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        getContentPane().setLayout(new BorderLayout());
        getContentPane().setBackground(UITheme.BACKGROUND);
        getContentPane().add(buildDashboard(), BorderLayout.NORTH);
        getContentPane().add(buildMain(), BorderLayout.CENTER);
        setMinimumSize(new Dimension(520, 420));
        setInitialWindowSize();
        setLocationRelativeTo(null);

        style();

        connectButton.addActionListener(e -> connect());
        resetWorkersButton.addActionListener(e -> resetWorkerDisplay());
        submitButton.addActionListener(e -> submitJob());
        resetJobsButton.addActionListener(e -> resetJobs());
        loadCsvButton.addActionListener(e -> loadCsv());
        clearButton.addActionListener(e -> {
            inputArea.setText("");
            log("Input cleared");
        });
        jobTypeCombo.addActionListener(e -> updateHint());
        inputArea.getDocument().addDocumentListener(new DocumentListener() {
            @Override
            public void insertUpdate(DocumentEvent e) {
                updateActionVisibility();
            }

            @Override
            public void removeUpdate(DocumentEvent e) {
                updateActionVisibility();
            }

            @Override
            public void changedUpdate(DocumentEvent e) {
                updateActionVisibility();
            }
        });
        completionProgress.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                showWorkerBreakdown();
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            workerRefreshTimer.stop();
            unregisterClientQuietly();
            channels.shutdown();
        }, "client-shutdown"));

        updateHint();
        refreshCompletionProgress();
        updateConnectButtonState();
        updateActionVisibility();
        log(clientName + " ready (" + clientId + "). Connect to the Bootstrap Node, then submit jobs "
                + "(they run in the background) over gRPC.");
    }

    private static String allocateClientName() {
        return "Client 1";
    }

    private static String readableClientId(String clientName) {
        String normalized = clientName.trim()
                .replaceAll("[^A-Za-z0-9]+", "-")
                .replaceAll("(^-+|-+$)", "")
                .toLowerCase();
        String prefix = normalized.isEmpty() ? "client" : normalized;
        return prefix + "-" + ProcessHandle.current().pid() + "-" + Long.toUnsignedString(System.nanoTime(), 36);
    }

    private void style() {
        UITheme.button(connectButton);
        UITheme.button(resetWorkersButton);
        UITheme.button(loadCsvButton);
        UITheme.button(clearButton);
        UITheme.button(submitButton);
        UITheme.button(resetJobsButton);
        UITheme.textField(hostField);
        UITheme.textField(bootstrapPortField);
        UITheme.textArea(inputArea);
        UITheme.textArea(logArea);
        inputArea.setLineWrap(true);
        inputArea.setWrapStyleWord(true);
        logArea.setLineWrap(true);
        logArea.setWrapStyleWord(true);
        UITheme.combo(jobTypeCombo);
        UITheme.table(taskTable);
        taskTable.setModel(tableModel);
        taskTable.setAutoResizeMode(JTable.AUTO_RESIZE_SUBSEQUENT_COLUMNS);
        taskTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        taskTable.setAutoCreateRowSorter(true);
        completionValue.setFont(UITheme.BOLD);
        completionValue.setForeground(UITheme.SUCCESS);
        completionValue.setHorizontalAlignment(SwingConstants.CENTER);
        completionProgress.setStringPainted(true);
        completionProgress.setForeground(UITheme.SUCCESS);
        completionProgress.setBackground(UITheme.INPUT);
        completionProgress.setBorder(BorderFactory.createLineBorder(UITheme.BORDER));
        completionProgress.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        completionProgress.setToolTipText("Click to show worker-level breakdown");
        UITheme.label(connectionStatus, UITheme.MUTED);
        UITheme.label(coordinatorLabel, UITheme.MUTED);
        logArea.setEditable(false);
    }

    private void setInitialWindowSize() {
        Rectangle screen = GraphicsEnvironment.getLocalGraphicsEnvironment().getMaximumWindowBounds();
        int width = Math.min(900, Math.max(560, screen.width - 120));
        int height = Math.min(700, Math.max(460, screen.height - 120));
        setSize(width, height);
    }

    // ---------------------------------------------------------------- dashboard

    private Component buildDashboard() {
        JPanel dashboard = new JPanel(new GridBagLayout());
        dashboard.setBackground(UITheme.BACKGROUND);
        dashboard.setBorder(BorderFactory.createEmptyBorder(4, 6, 2, 6));

        statusPill.setFont(UITheme.SECTION);
        statusPill.setForeground(UITheme.ERROR);
        statusPill.setHorizontalAlignment(SwingConstants.CENTER);
        statusPill.setOpaque(true);
        statusPill.setBackground(UITheme.CARD);
        updateStatusPill(UITheme.ERROR);
        coordinatorValue.setFont(UITheme.SECTION);
        coordinatorValue.setForeground(UITheme.TEXT);
        workersValue.setFont(UITheme.SECTION);
        workersValue.setForeground(UITheme.TEXT);
        workerListValue.setFont(UITheme.BASE);
        workerListValue.setForeground(UITheme.MUTED);
        jobsSubmittedValue.setFont(UITheme.SECTION);
        jobsSubmittedValue.setForeground(UITheme.ACCENT);
        JLabel title = new JLabel(clientName);
        title.setFont(UITheme.SECTION);
        title.setForeground(UITheme.TEXT);

        GridBagConstraints g = new GridBagConstraints();
        g.gridy = 0;
        g.insets = new Insets(2, 6, 2, 6);
        g.anchor = GridBagConstraints.WEST;
        g.gridx = 0;
        g.weightx = 0.8;
        dashboard.add(title, g);
        g.gridx = 1;
        g.weightx = 0;
        dashboard.add(metric("Status", statusPill), g);
        g.gridx = 2;
        dashboard.add(metric("Coordinator", coordinatorValue), g);
        g.gridx = 3;
        dashboard.add(metric("Workers", workersValue), g);
        g.gridx = 4;
        g.weightx = 1.0;
        dashboard.add(metric("Nodes", workerListValue), g);
        g.gridx = 5;
        g.weightx = 0;
        dashboard.add(metric("Jobs", jobsSubmittedValue), g);
        g.gridx = 6;
        dashboard.add(metric("Overall", completionValue), g);

        g.gridx = 0;
        g.gridy = 1;
        g.gridwidth = 7;
        g.fill = GridBagConstraints.HORIZONTAL;
        g.weightx = 1;
        g.insets = new Insets(0, 6, 2, 6);
        dashboard.add(completionProgress, g);
        return dashboard;
    }

    private static JPanel metric(String labelText, Component value) {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setOpaque(false);
        JLabel label = new JLabel(labelText + ":");
        label.setFont(UITheme.BOLD);
        label.setForeground(UITheme.MUTED);
        GridBagConstraints c = new GridBagConstraints();
        c.gridx = 0;
        c.gridy = 0;
        c.insets = new Insets(0, 0, 0, 4);
        c.anchor = GridBagConstraints.WEST;
        panel.add(label, c);
        c.gridx = 1;
        c.weightx = 1;
        panel.add(value, c);
        return panel;
    }

    // ---------------------------------------------------------------- panels

    private Component buildMain() {
        JPanel connectionPanel = new JPanel(new GridBagLayout());
        connectionPanel.setBackground(UITheme.PANEL);
        connectionPanel.setBorder(UITheme.titledBorder("Connection"));
        JLabel hostLabel = new JLabel("Bootstrap host:");
        UITheme.label(hostLabel, UITheme.MUTED);
        JLabel portLabel = new JLabel("Port:");
        UITheme.label(portLabel, UITheme.MUTED);
        addConnectionControl(connectionPanel, hostLabel, 0, 0, 0);
        addConnectionControl(connectionPanel, hostField, 1, 0, 1);
        addConnectionControl(connectionPanel, portLabel, 2, 0, 0);
        addConnectionControl(connectionPanel, bootstrapPortField, 3, 0, 0);
        addConnectionControl(connectionPanel, connectButton, 4, 0, 0);
        addConnectionControl(connectionPanel, resetWorkersButton, 5, 0, 0);
        GridBagConstraints statusGbc = connectionConstraints(0, 1, 6, 1);
        statusGbc.fill = GridBagConstraints.HORIZONTAL;
        connectionPanel.add(connectionStatus, statusGbc);

        JPanel jobPanel = new JPanel(new BorderLayout(8, 8));
        jobPanel.setBackground(UITheme.PANEL);
        jobPanel.setBorder(UITheme.titledBorder("Job"));

        JPanel jobHeader = new JPanel(new GridBagLayout());
        jobHeader.setBackground(UITheme.PANEL);
        JLabel typeLabel = new JLabel("Job type:");
        UITheme.label(typeLabel, UITheme.MUTED);
        addConnectionControl(jobHeader, typeLabel, 0, 0, 0);
        addConnectionControl(jobHeader, jobTypeCombo, 1, 0, 0);
        addConnectionControl(jobHeader, coordinatorLabel, 2, 0, 1);
        addConnectionControl(jobHeader, loadCsvButton, 3, 0, 0);
        addConnectionControl(jobHeader, clearButton, 4, 0, 0);
        addConnectionControl(jobHeader, resetJobsButton, 5, 0, 0);
        addConnectionControl(jobHeader, submitButton, 6, 0, 0);

        JPanel inputPanel = new JPanel(new BorderLayout());
        inputPanel.setBackground(UITheme.PANEL);
        JScrollPane inputScroll = new JScrollPane(inputArea);
        inputScroll.setPreferredSize(new Dimension(0, 64));
        UITheme.scroll(inputScroll);
        inputPanel.add(inputScroll, BorderLayout.CENTER);

        jobPanel.add(jobHeader, BorderLayout.NORTH);
        jobPanel.add(inputPanel, BorderLayout.CENTER);

        JPanel top = new JPanel(new GridBagLayout());
        top.setBackground(UITheme.BACKGROUND);
        GridBagConstraints topGbc = new GridBagConstraints();
        topGbc.gridx = 0;
        topGbc.gridy = 0;
        topGbc.weightx = 1;
        topGbc.fill = GridBagConstraints.HORIZONTAL;
        topGbc.insets = new Insets(4, 6, 3, 6);
        top.add(connectionPanel, topGbc);
        topGbc.gridy = 1;
        topGbc.weighty = 0;
        topGbc.fill = GridBagConstraints.HORIZONTAL;
        topGbc.insets = new Insets(3, 6, 4, 6);
        top.add(jobPanel, topGbc);

        JPanel tablePanel = new JPanel(new BorderLayout());
        tablePanel.setBackground(UITheme.PANEL);
        tablePanel.setBorder(UITheme.titledBorder("Tasks (concurrent submissions supported)"));
        JScrollPane tableScroll = new JScrollPane(taskTable);
        UITheme.scroll(tableScroll);
        tablePanel.add(tableScroll, BorderLayout.CENTER);
        tablePanel.setMinimumSize(new Dimension(0, 0));

        JPanel logPanel = new JPanel(new BorderLayout());
        logPanel.setBackground(UITheme.PANEL);
        logPanel.setBorder(UITheme.titledBorder("Log"));
        JScrollPane logScroll = new JScrollPane(logArea);
        UITheme.scroll(logScroll);
        logPanel.add(logScroll, BorderLayout.CENTER);
        logPanel.setMinimumSize(new Dimension(0, 0));

        JPanel main = new JPanel(new GridBagLayout());
        main.setBackground(UITheme.BACKGROUND);
        GridBagConstraints mainGbc = new GridBagConstraints();
        mainGbc.gridx = 0;
        mainGbc.gridy = 0;
        mainGbc.weightx = 1;
        mainGbc.weighty = 0;
        mainGbc.fill = GridBagConstraints.HORIZONTAL;
        main.add(top, mainGbc);
        mainGbc.gridy = 1;
        mainGbc.weighty = 0.62;
        mainGbc.fill = GridBagConstraints.BOTH;
        mainGbc.insets = new Insets(0, 6, 3, 6);
        main.add(tablePanel, mainGbc);
        mainGbc.gridy = 2;
        mainGbc.weighty = 0.38;
        mainGbc.insets = new Insets(3, 6, 6, 6);
        main.add(logPanel, mainGbc);
        return main;
    }

    private static void addConnectionControl(JPanel panel, Component component, int x, int y, double weightx) {
        GridBagConstraints c = connectionConstraints(x, y, 1, weightx);
        c.fill = weightx > 0 ? GridBagConstraints.HORIZONTAL : GridBagConstraints.NONE;
        panel.add(component, c);
    }

    private static GridBagConstraints connectionConstraints(int x, int y, int width, double weightx) {
        GridBagConstraints c = new GridBagConstraints();
        c.gridx = x;
        c.gridy = y;
        c.gridwidth = width;
        c.weightx = weightx;
        c.insets = new Insets(2, 4, 2, 4);
        c.anchor = GridBagConstraints.WEST;
        return c;
    }

    private void updateHint() {
        String type = (String) jobTypeCombo.getSelectedItem();
        inputArea.setToolTipText("PRIMESUM".equals(type) ? "start,end  e.g. 1,1000"
                : "comma-separated integers  e.g. 12,5,99,2,17");
    }

    // ---------------------------------------------------------------- connection

    private void connect() {
        if (bootstrap != null) {
            return;
        }
        bootstrapHost = hostField.getText().trim().isEmpty() ? "localhost" : hostField.getText().trim();
        try {
            bootstrapPort = Integer.parseInt(bootstrapPortField.getText().trim());
        } catch (NumberFormatException e) {
            appendError("Bootstrap port must be a number");
            return;
        }

        connectInProgress = true;
        updateConnectButtonState();
        connectionStatus.setForeground(UITheme.WARNING);
        connectionStatus.setText("Connecting...");
        jobExecutor.submit(this::doConnect);
    }

    private void resetJobs() {
        jobSequence.set(0);
        completedJobs.clear();
        tableModel.clear();
        jobsSubmittedValue.setText("0");
        refreshCompletionProgress();
        updateActionVisibility();
        log("Job table reset");
    }

    private void resetWorkerDisplay() {
        coordinator = null;
        activeWorkerCount = 0;
        latestCoordinatorId = WorkerClusterConfig.NO_COORDINATOR;
        latestCoordinatorTermJobs = 0;
        latestWorkers.clear();
        workersValue.setText("0 active");
        workerListValue.setText("none");
        coordinatorLabel.setText("Coordinator: none");
        coordinatorValue.setText("None");
        coordinatorValue.setForeground(UITheme.MUTED);
        updateStatusPill(UITheme.MUTED);
        connectionStatus.setForeground(UITheme.WARNING);
        connectionStatus.setText(bootstrap == null ? "Worker display reset" : "Worker display reset - refreshing");
        updateActionVisibility();
        log("Worker display reset");
        if (bootstrap != null) {
            refreshWorkerStatusAsync();
        }
    }

    private void updateActionVisibility() {
        boolean hasInput = !inputArea.getText().trim().isEmpty();
        boolean hasJobs = tableModel.getRowCount() > 0;
        boolean hasWorkerState = bootstrap != null || activeWorkerCount > 0 || coordinator != null;

        submitButton.setVisible(hasInput);
        submitButton.setEnabled(hasInput && bootstrap != null);
        clearButton.setVisible(hasInput);
        loadCsvButton.setVisible(!hasInput);
        resetJobsButton.setVisible(hasJobs);
        resetWorkersButton.setVisible(hasWorkerState);

        revalidate();
        repaint();
    }

    private void doConnect() {
        try {
            BootstrapServiceGrpc.BootstrapServiceBlockingStub stored =
                    BootstrapServiceGrpc.newBlockingStub(channels.channel(bootstrapHost, bootstrapPort));
            BootstrapServiceGrpc.BootstrapServiceBlockingStub probe =
                    stored.withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            ClusterStatus status = probe.getClusterStatus(EMPTY);
            List<WorkerInfo> active = workerInfoList(status);
            activeWorkerCount = active.size();
            boolean clientRegistered = registerClientIfSupported(probe);
            bootstrap = stored;

            int coordinatorId = status.getCoordinatorId();
            if (coordinatorId == WorkerClusterConfig.NO_COORDINATOR && activeWorkerCount > 0) {
                active = probe.getActiveWorkers(EMPTY).getWorkersList();
                activeWorkerCount = active.size();
                coordinatorId = waitForCoordinator(active, probe);
            }
            coordinator = coordinatorId == WorkerClusterConfig.NO_COORDINATOR
                    ? null
                    : lookupRegistered(active, coordinatorId);

            final int elected = coordinatorId;
            final List<WorkerInfo> displayedWorkers = new ArrayList<>(active);
            final int displayedTermJobs = coordinatorTermJobs(displayedWorkers, elected);
            SwingUtilities.invokeLater(() -> {
                connectInProgress = false;
                updateWorkerDisplay(activeWorkerCount, displayedWorkers, elected, displayedTermJobs);
                if (elected == WorkerClusterConfig.NO_COORDINATOR) {
                    connectionStatus.setForeground(UITheme.WARNING);
                    connectionStatus.setText(activeWorkerCount == 0
                            ? "Connected - waiting for workers"
                            : "Connected - no coordinator yet");
                    coordinatorLabel.setText("Coordinator: none (workers are electing...)");
                    coordinatorValue.setText("None");
                    coordinatorValue.setForeground(UITheme.WARNING);
                    updateStatusPill(UITheme.WARNING);
                } else {
                    connectionStatus.setForeground(UITheme.SUCCESS);
                    connectionStatus.setText("Online - coordinator worker " + elected);
                    coordinatorLabel.setText("Coordinator: worker " + elected);
                    coordinatorValue.setText("Worker " + elected);
                    coordinatorValue.setForeground(UITheme.SUCCESS);
                    updateStatusPill(UITheme.SUCCESS);
                }
                updateConnectButtonState();
                updateActionVisibility();
                log("Connected to Bootstrap Node at " + bootstrapHost + ":" + bootstrapPort
                        + " as " + clientName + " (" + clientId + ")"
                        + " - " + activeWorkerCount + " active worker(s)"
                        + (elected == WorkerClusterConfig.NO_COORDINATOR
                        ? ", no coordinator elected yet" : ", coordinator is worker " + elected));
                log("Bootstrap active-worker registry is used for worker discovery only; "
                        + "leader election is performed by worker nodes.");
                if (!clientRegistered) {
                    log("Bootstrap does not support client registration yet; restart it with the rebuilt bootstrap.jar "
                            + "to show client labels in the bootstrap terminal.");
                }
                workerRefreshTimer.start();
            });
        } catch (Exception e) {
            SwingUtilities.invokeLater(() -> {
                bootstrap = null;
                coordinator = null;
                workerRefreshTimer.stop();
                connectInProgress = false;
                updateConnectButtonState();
                connectionStatus.setForeground(UITheme.ERROR);
                connectionStatus.setText("Connection failed");
                updateStatusPill(UITheme.ERROR);
                appendError("Could not connect: " + rootMessage(e));
            });
        }
    }

    private void refreshWorkerStatusAsync() {
        if (workerRefreshInProgress) {
            return;
        }
        BootstrapServiceGrpc.BootstrapServiceBlockingStub currentBootstrap = bootstrap;
        if (currentBootstrap == null) {
            try {
                String host = hostField.getText().trim().isEmpty() ? "localhost" : hostField.getText().trim();
                int port = Integer.parseInt(bootstrapPortField.getText().trim());
                currentBootstrap = BootstrapServiceGrpc.newBlockingStub(channels.channel(host, port));
            } catch (RuntimeException e) {
                return;
            }
        }

        workerRefreshInProgress = true;
        BootstrapServiceGrpc.BootstrapServiceBlockingStub refreshBootstrap = currentBootstrap;
        jobExecutor.submit(() -> {
            try {
                ClusterStatus status = refreshBootstrap
                        .withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .getClusterStatus(EMPTY);
                List<WorkerInfo> active = workerInfoList(status);
                activeWorkerCount = active.size();
                int termJobs = coordinatorTermJobs(active, status.getCoordinatorId());
                SwingUtilities.invokeLater(() ->
                        updateWorkerDisplay(active.size(), active, status.getCoordinatorId(), termJobs));
            } catch (RuntimeException ignored) {
                // The bootstrap may be starting or temporarily unavailable; keep the last visible state.
            } finally {
                workerRefreshInProgress = false;
            }
        });
    }

    private void updateWorkerDisplay(int activeCount, List<WorkerInfo> workers, int coordinatorId, int termJobs) {
        activeWorkerCount = activeCount;
        latestWorkers.clear();
        latestWorkers.addAll(workers == null ? List.of() : workers);
        latestCoordinatorId = coordinatorId;
        latestCoordinatorTermJobs = Math.max(0, termJobs);
        workersValue.setText(activeCount + " active");
        workerListValue.setText(formatWorkerLabels(workers, coordinatorId));
        if (!connectInProgress) {
            if (coordinatorId == WorkerClusterConfig.NO_COORDINATOR) {
                coordinator = null;
                coordinatorLabel.setText("Coordinator: none");
                coordinatorValue.setText("None");
                coordinatorValue.setForeground(activeCount == 0 ? UITheme.MUTED : UITheme.WARNING);
                updateStatusPill(activeCount == 0 ? UITheme.MUTED : UITheme.WARNING);
                connectionStatus.setForeground(UITheme.SUCCESS);
                connectionStatus.setText(activeCount == 0 ? "Connected - waiting for workers"
                        : "Connected - workers are electing");
            } else {
                try {
                    coordinator = lookupRegistered(workers, coordinatorId);
                } catch (IllegalStateException e) {
                    coordinator = null;
                    coordinatorId = WorkerClusterConfig.NO_COORDINATOR;
                    coordinatorLabel.setText("Coordinator: none");
                    coordinatorValue.setText("None");
                    coordinatorValue.setForeground(UITheme.WARNING);
                    updateStatusPill(UITheme.WARNING);
                    connectionStatus.setForeground(UITheme.WARNING);
                    connectionStatus.setText("Connected - coordinator not reachable");
                    updateConnectButtonState();
                    updateActionVisibility();
                    return;
                }
                coordinatorValue.setText("Worker " + coordinatorId);
                coordinatorValue.setForeground(UITheme.SUCCESS);
                coordinatorLabel.setText("Coordinator: worker " + coordinatorId);
                updateStatusPill(UITheme.SUCCESS);
                connectionStatus.setForeground(UITheme.SUCCESS);
                connectionStatus.setText("Online - coordinator worker " + coordinatorId);
            }
        }
        updateConnectButtonState();
        updateActionVisibility();
    }

    private void updateStatusPill(java.awt.Color color) {
        statusPill.setText("  O  ");
        statusPill.setForeground(color);
        statusPill.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(color, 2),
                BorderFactory.createEmptyBorder(4, 8, 4, 8)));
    }

    private void updateConnectButtonState() {
        boolean connected = bootstrap != null;
        connectButton.setText(connectInProgress ? "Connecting..." : connected ? "Connected" : "Connect");
        connectButton.setEnabled(!connectInProgress && !connected);
        hostField.setEnabled(!connectInProgress && !connected);
        bootstrapPortField.setEnabled(!connectInProgress && !connected);
    }

    private static String formatWorkerLabels(List<WorkerInfo> workers, int coordinatorId) {
        if (workers == null || workers.isEmpty()) {
            return "none";
        }
        List<WorkerInfo> sorted = new ArrayList<>(workers);
        sorted.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
        List<String> labels = new ArrayList<>();
        for (WorkerInfo worker : sorted) {
            labels.add("worker " + worker.getWorkerId() + " RUNNING"
                    + (worker.getWorkerId() == coordinatorId ? " (coordinator)" : ""));
        }
        return String.join(", ", labels);
    }

    private static List<WorkerInfo> workerInfoList(ClusterStatus status) {
        List<WorkerInfo> workers = new ArrayList<>();
        for (WorkerStatus worker : status.getWorkersList()) {
            workers.add(WorkerInfo.newBuilder()
                    .setWorkerId(worker.getWorkerId())
                    .setHost(worker.getHost())
                    .setPort(worker.getPort())
                    .build());
        }
        return workers;
    }

    private int coordinatorTermJobs(List<WorkerInfo> workers, int coordinatorId) {
        if (coordinatorId == WorkerClusterConfig.NO_COORDINATOR || workers == null) {
            return 0;
        }
        for (WorkerInfo worker : workers) {
            if (worker.getWorkerId() != coordinatorId) {
                continue;
            }
            try {
                IntValue termJobs = probeWorker(worker).getJobsThisTerm(EMPTY);
                return Math.max(0, Math.min(5, termJobs.getValue()));
            } catch (RuntimeException e) {
                return 0;
            }
        }
        return 0;
    }

    private boolean registerClientIfSupported(BootstrapServiceGrpc.BootstrapServiceBlockingStub probe) {
        try {
            probe.registerClient(ClientInfo.newBuilder()
                    .setClientId(clientId)
                    .setName(clientName)
                    .setHost(bootstrapHost)
                    .setConnectedAtMillis(System.currentTimeMillis())
                    .build());
            return true;
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.UNIMPLEMENTED) {
                return false;
            }
            throw e;
        }
    }

    private void unregisterClientQuietly() {
        BootstrapServiceGrpc.BootstrapServiceBlockingStub currentBootstrap = bootstrap;
        if (currentBootstrap == null) {
            return;
        }
        try {
            currentBootstrap
                    .withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .unregisterClient(StringValue.newBuilder().setValue(clientId).build());
        } catch (Exception ignored) {
            // The bootstrap may already be stopped; shutdown should stay quiet.
        }
    }

    /**
     * Asks every registered worker which coordinator it knows and returns the
     * agreed coordinator id (or {@link WorkerClusterConfig#NO_COORDINATOR} if none).
     */
    private int findCoordinatorId(List<WorkerInfo> active) {
        for (WorkerInfo info : active) {
            WorkerServiceGrpc.WorkerServiceBlockingStub worker = probeWorker(info);
            if (worker == null) {
                continue;
            }
            int candidate = worker.getCurrentCoordinatorId(EMPTY).getValue();
            if (candidate != WorkerClusterConfig.NO_COORDINATOR) {
                return candidate;
            }
        }
        return WorkerClusterConfig.NO_COORDINATOR;
    }

    private int waitForCoordinator(List<WorkerInfo> active,
                                   BootstrapServiceGrpc.BootstrapServiceBlockingStub probe) {
        return waitForCoordinator(active, probe, COORDINATOR_WAIT_MILLIS);
    }

    private int waitForCoordinator(List<WorkerInfo> active,
                                   BootstrapServiceGrpc.BootstrapServiceBlockingStub probe,
                                   long waitMillis) {
        long deadline = System.currentTimeMillis() + waitMillis;
        List<WorkerInfo> latest = active;
        while (System.currentTimeMillis() <= deadline) {
            int coordinatorId = findCoordinatorId(latest);
            if (coordinatorId != WorkerClusterConfig.NO_COORDINATOR) {
                return coordinatorId;
            }
            try {
                Thread.sleep(COORDINATOR_POLL_MILLIS);
                latest = probe.getActiveWorkers(EMPTY).getWorkersList();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return WorkerClusterConfig.NO_COORDINATOR;
            } catch (RuntimeException e) {
                return WorkerClusterConfig.NO_COORDINATOR;
            }
        }
        return WorkerClusterConfig.NO_COORDINATOR;
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub lookupRegistered(List<WorkerInfo> active, int workerId) {
        for (WorkerInfo info : active) {
            if (info.getWorkerId() == workerId) {
                WorkerServiceGrpc.WorkerServiceBlockingStub worker = jobWorker(info);
                if (worker != null) {
                    return worker;
                }
            }
        }
        throw new IllegalStateException("Coordinator worker " + workerId + " is not reachable");
    }

    /** A stub for short probes (connect / refresh), guarded by a deadline. */
    private WorkerServiceGrpc.WorkerServiceBlockingStub probeWorker(WorkerInfo info) {
        try {
            return WorkerServiceGrpc.newBlockingStub(channels.channel(info.getHost(), info.getPort()))
                    .withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.err.println("[Client] cannot reach " + info + ": " + e.getMessage());
            return null;
        }
    }

    /** A stub for job submission; no deadline so long-running jobs may finish. */
    private WorkerServiceGrpc.WorkerServiceBlockingStub jobWorker(WorkerInfo info) {
        try {
            return WorkerServiceGrpc.newBlockingStub(channels.channel(info.getHost(), info.getPort()));
        } catch (Exception e) {
            System.err.println("[Client] cannot reach " + info + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Re-queries the Bootstrap Node for the currently elected coordinator. Used
     * when a 5-job term ends so the client automatically follows the rotation to
     * whichever worker is elected next - no manual reconnect needed.
     */
    private boolean refreshCoordinator() {
        BootstrapServiceGrpc.BootstrapServiceBlockingStub currentBootstrap = bootstrap;
        if (currentBootstrap == null) {
            return false;
        }
        try {
            List<WorkerInfo> active = currentBootstrap
                    .withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .getActiveWorkers(EMPTY).getWorkersList();
            int coordinatorId = findCoordinatorId(active);
            coordinator = coordinatorId == WorkerClusterConfig.NO_COORDINATOR
                    ? null
                    : lookupRegistered(active, coordinatorId);
            final int elected = coordinatorId;
            SwingUtilities.invokeLater(() -> {
                if (elected == WorkerClusterConfig.NO_COORDINATOR) {
                    coordinatorLabel.setText("Coordinator: none (workers are electing...)");
                    coordinatorValue.setText("None");
                    coordinatorValue.setForeground(UITheme.WARNING);
                    statusPill.setText("  O  ");
                    statusPill.setForeground(UITheme.WARNING);
                } else {
                    coordinatorLabel.setText("Coordinator: worker " + elected);
                    coordinatorValue.setText("Worker " + elected);
                    coordinatorValue.setForeground(UITheme.SUCCESS);
                    statusPill.setText("  O  ");
                    statusPill.setForeground(UITheme.SUCCESS);
                }
                updateActionVisibility();
            });
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub resolveCoordinatorForJob(long waitMillis) {
        BootstrapServiceGrpc.BootstrapServiceBlockingStub currentBootstrap = bootstrap;
        if (currentBootstrap == null) {
            throw new IllegalStateException("No bootstrap connection");
        }
        BootstrapServiceGrpc.BootstrapServiceBlockingStub probe =
                currentBootstrap.withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        List<WorkerInfo> active = probe.getActiveWorkers(EMPTY).getWorkersList();
        int coordinatorId = waitForCoordinator(active, probe, waitMillis);
        if (coordinatorId == WorkerClusterConfig.NO_COORDINATOR) {
            coordinator = null;
            throw new IllegalStateException("No coordinator elected yet");
        }
        coordinator = lookupRegistered(active, coordinatorId);
        final int elected = coordinatorId;
        SwingUtilities.invokeLater(() -> {
            coordinatorLabel.setText("Coordinator: worker " + elected);
            coordinatorValue.setText("Worker " + elected);
            coordinatorValue.setForeground(UITheme.SUCCESS);
            connectionStatus.setForeground(UITheme.SUCCESS);
            connectionStatus.setText("Online - coordinator worker " + elected);
            updateActionVisibility();
        });
        return coordinator;
    }

    // ---------------------------------------------------------------- jobs

    private void submitJob() {
        if (bootstrap == null) {
            appendError("Connect to Bootstrap before submitting jobs");
            return;
        }

        String type = (String) jobTypeCombo.getSelectedItem();
        String raw = inputArea.getText().trim();
        if (raw.isEmpty()) {
            appendError("Input is empty");
            return;
        }

        final int jobId = jobSequence.incrementAndGet();
        final String submitted = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));

        addTask(jobId, type, submitted, "Running", "-", "-", "-", null);
        jobsSubmittedValue.setText(String.valueOf(jobSequence.get()));
        refreshCompletionProgress();

        jobExecutor.submit(() -> {
            WorkerServiceGrpc.WorkerServiceBlockingStub targetWorker;
            try {
                targetWorker = resolveCoordinatorForJob(COORDINATOR_HANDOFF_WAIT_MILLIS);
                showAssignedCoordinator(jobId, targetWorker);
            } catch (Exception e) {
                String message = rootMessage(e);
                updateTask(jobId, "Failed", "-", "-", "-", message);
                log("Job " + jobId + " [" + type + "] failed -> " + message);
                return;
            }
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    TaskResult result = dispatch(targetWorker, type, raw);
                    String workers = joinWorkerIds(result.getWorkerIdsList());
                    String resultText = "task=" + shortTaskId(result.getTaskId()) + " result=" + result.getResult();
                    updateTask(jobId, "Done", "worker " + result.getCoordinatorId(), workers, resultText, null);
                    log("Job " + jobId + " [" + type + "] finished -> " + result);
                    refreshCoordinator();
                    return;
                } catch (Exception e) {
                    String message = rootMessage(e);
                    boolean coordinatorChanged = attempt < 2 && needsNewCoordinator(message);
                    if (coordinatorChanged) {
                        log("Term hand-over detected - re-resolving the new coordinator and retrying job " + jobId);
                        try {
                            targetWorker = resolveCoordinatorForJob(COORDINATOR_HANDOFF_WAIT_MILLIS);
                            showAssignedCoordinator(jobId, targetWorker);
                        } catch (Exception resolveFailure) {
                            message = rootMessage(resolveFailure);
                            updateTask(jobId, "Failed", "-", "-", "-", message);
                            log("Job " + jobId + " [" + type + "] failed -> " + message);
                            return;
                        }
                        continue;
                    }
                    updateTask(jobId, "Failed", "-", "-", "-",
                            coordinatorChanged ? message + " (coordinator re-elected)" : message);
                    log("Job " + jobId + " [" + type + "] failed -> " + message);
                    return;
                }
            }
        });
    }

    private void showAssignedCoordinator(int jobId, WorkerServiceGrpc.WorkerServiceBlockingStub targetWorker) {
        int assignedCoordinator = currentCoordinatorId(targetWorker);
        if (assignedCoordinator != WorkerClusterConfig.NO_COORDINATOR) {
            updateTask(jobId, "Running", "worker " + assignedCoordinator, "-", "-", null);
        }
    }

    private int currentCoordinatorId(WorkerServiceGrpc.WorkerServiceBlockingStub worker) {
        try {
            return worker.withDeadlineAfter(PROBE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .getCurrentCoordinatorId(EMPTY).getValue();
        } catch (Exception e) {
            return WorkerClusterConfig.NO_COORDINATOR;
        }
    }

    /** True when the failure means the elected coordinator changed or is still being chosen. */
    private boolean needsNewCoordinator(String message) {
        return message != null && (message.contains("not the coordinator")
                || message.contains("Coordinator term ended")
                || message.contains("No coordinator elected yet"));
    }

    private TaskResult dispatch(WorkerServiceGrpc.WorkerServiceBlockingStub target, String type, String raw) {
        switch (type) {
            case "MAX":
                return target.submitMaxJob(MaxJobRequest.newBuilder()
                        .setClientId(clientId)
                        .setNumbers(NumberList.newBuilder().addAllNumbers(parseNumbers(raw)).build())
                        .build());
            case "PRIMECOUNT":
                return target.submitPrimeCount(PrimeCountRequest.newBuilder()
                        .setClientId(clientId)
                        .setNumbers(NumberList.newBuilder().addAllNumbers(parseNumbers(raw)).build())
                        .build());
            case "PRIMESUM":
                int[] range = parseRange(raw);
                return target.submitPrimeSum(PrimeSumRequest.newBuilder()
                        .setClientId(clientId)
                        .setStart(range[0])
                        .setEnd(range[1])
                        .build());
            default:
                throw new IllegalArgumentException("Unknown job type: " + type);
        }
    }

    private static String joinWorkerIds(List<Integer> workerIds) {
        if (workerIds == null || workerIds.isEmpty()) {
            return "-";
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < workerIds.size(); i++) {
            if (i > 0) {
                builder.append(",");
            }
            builder.append(workerIds.get(i));
        }
        return builder.toString();
    }

    private static String shortTaskId(String taskId) {
        return taskId != null && taskId.length() > 8 ? taskId.substring(0, 8) : String.valueOf(taskId);
    }

    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        FileNameExtensionFilter csvFilter = new FileNameExtensionFilter("CSV files (*.csv)", "csv");
        chooser.setAcceptAllFileFilterUsed(true);
        chooser.addChoosableFileFilter(csvFilter);
        chooser.setFileFilter(csvFilter);
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        Path path = chooser.getSelectedFile().toPath();
        try {
            String content = normalizeCsvForInput(Files.readString(path));
            inputArea.setText(content);
            log("Loaded CSV from " + path.getFileName());
        } catch (IOException e) {
            appendError("Could not read CSV file: " + e.getMessage());
        }
    }

    private static String normalizeCsvForInput(String raw) {
        List<String> values = new ArrayList<>();
        for (String part : raw.split("[,\\s]+")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        return String.join(", ", values);
    }

    /** Parses "12,5,99,2,17" into a list of integers. */
    static List<Integer> parseNumbers(String raw) {
        List<Integer> numbers = new ArrayList<>();
        for (String part : raw.split("[,\\s]+")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                numbers.add(Integer.parseInt(trimmed));
            }
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("no numbers found in input");
        }
        return numbers;
    }

    /** Parses "1,1000" into a start/end range. */
    static int[] parseRange(String raw) {
        List<Integer> parts = parseParts(raw);
        if (parts.size() < 2) {
            throw new IllegalArgumentException("PRIMESUM needs two values: start,end");
        }
        return new int[]{parts.get(0), parts.get(1)};
    }

    private static List<Integer> parseParts(String raw) {
        List<Integer> parts = new ArrayList<>();
        for (String part : raw.split("[,\\s]+")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                parts.add(Integer.parseInt(trimmed));
            }
        }
        return parts;
    }

    private String rootMessage(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null ? current.getClass().getSimpleName() : message;
    }

    private void addTask(int jobId, String type, String submitted, String status, String coordinator, String workers, String result, String error) {
        SwingUtilities.invokeLater(() -> {
            tableModel.addRow(jobId, type, submitted, status, coordinator, workers, result, error);
            updateActionVisibility();
        });
    }

    private void updateTask(int jobId, String status, String coordinator, String workers, String result, String error) {
        if (isTerminalStatus(status)) {
            completedJobs.add(jobId);
        }
        SwingUtilities.invokeLater(() -> {
            tableModel.updateRow(jobId, status, coordinator, workers, result, error);
            refreshCompletionProgress();
            updateActionVisibility();
        });
    }

    private static boolean isTerminalStatus(String status) {
        return "Done".equals(status) || "Failed".equals(status);
    }

    private void refreshCompletionProgress() {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(this::refreshCompletionProgress);
            return;
        }
        int submitted = jobSequence.get();
        int completed = completedJobs.size();
        int percent = submitted == 0 ? 0 : Math.round((completed * 100.0f) / submitted);
        completionValue.setText(percent + "%");
        completionProgress.setValue(percent);
        completionProgress.setString(completed + "/" + submitted + " complete");
    }

    private static int termNumberForJob(int jobId) {
        return ((jobId - 1) / 5) + 1;
    }

    private static int termSlotForJob(int jobId) {
        return ((jobId - 1) % 5) + 1;
    }

    private static String termLabelForJob(int jobId) {
        return "T" + termNumberForJob(jobId) + " " + termSlotForJob(jobId) + "/5";
    }

    private void showWorkerBreakdown() {
        List<TermBreakdown> terms = tableModel.termBreakdowns();
        Map<Integer, Integer> workerJobCounts = tableModel.completedWorkerJobCounts();
        StringBuilder text = new StringBuilder();
        text.append("Progress: ")
                .append(completedJobs.size())
                .append("/")
                .append(jobSequence.get())
                .append(" jobs complete")
                .append(System.lineSeparator())
                .append(System.lineSeparator());

        if (terms.isEmpty()) {
            text.append("No submitted jobs are available for term breakdown yet.")
                    .append(System.lineSeparator())
                    .append(System.lineSeparator());
        } else {
            for (TermBreakdown term : terms) {
                text.append("Term ")
                        .append(term.termNumber)
                        .append(" coordinator: ")
                        .append(term.coordinator())
                        .append(System.lineSeparator())
                        .append("  jobs in this term: ")
                        .append(term.jobCount)
                        .append("/5")
                        .append(System.lineSeparator());
                if (term.workerCounts.isEmpty()) {
                    text.append("  worker breakdown: none yet")
                            .append(System.lineSeparator());
                } else {
                    text.append("  worker breakdown:")
                            .append(System.lineSeparator());
                    for (Map.Entry<Integer, Integer> entry : term.workerCounts.entrySet()) {
                        text.append("    worker ")
                                .append(entry.getKey())
                                .append(": ")
                                .append(entry.getValue())
                                .append(" job(s)")
                                .append(System.lineSeparator());
                    }
                }
                text.append(System.lineSeparator());
            }
        }

        if (latestWorkers.isEmpty()) {
            text.append("No active workers are currently reported by Bootstrap.");
        } else {
            List<WorkerInfo> sorted = new ArrayList<>(latestWorkers);
            sorted.sort(Comparator.comparingInt(WorkerInfo::getWorkerId));
            for (WorkerInfo worker : sorted) {
                int workerId = worker.getWorkerId();
                text.append("worker ")
                        .append(workerId)
                        .append(workerId == latestCoordinatorId ? "  COORDINATOR" : "")
                        .append(System.lineSeparator())
                        .append("  status: RUNNING")
                        .append(System.lineSeparator())
                        .append("  address: ")
                        .append(worker.getHost())
                        .append(":")
                        .append(worker.getPort())
                        .append(System.lineSeparator())
                        .append("  completed jobs participated in: ")
                        .append(workerJobCounts.getOrDefault(workerId, 0))
                        .append(System.lineSeparator())
                        .append(System.lineSeparator());
            }
        }

        JTextArea breakdown = new JTextArea(text.toString(), 14, 48);
        breakdown.setEditable(false);
        UITheme.textArea(breakdown);
        breakdown.setCaretPosition(0);
        JScrollPane scroll = new JScrollPane(breakdown);
        UITheme.scroll(scroll);
        JOptionPane.showMessageDialog(this, scroll, "Worker-Level Breakdown", JOptionPane.INFORMATION_MESSAGE);
    }

    private void log(String message) {
        logArea.append("[" + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] " + message + "\n");
        logArea.setCaretPosition(logArea.getDocument().getLength());
    }

    private void appendError(String message) {
        log("ERROR: " + message);
        JOptionPane.showMessageDialog(this, message, "Error", JOptionPane.ERROR_MESSAGE);
    }

    private static final class TermBreakdown {
        final int termNumber;
        int jobCount;
        String coordinator = "-";
        boolean coordinatorChanged = false;
        final Map<Integer, Integer> workerCounts = new LinkedHashMap<>();

        TermBreakdown(int termNumber) {
            this.termNumber = termNumber;
        }

        void addJob(String rowCoordinator, String workers) {
            jobCount++;
            if (rowCoordinator != null && !rowCoordinator.isBlank() && !"-".equals(rowCoordinator)) {
                if ("-".equals(coordinator)) {
                    coordinator = rowCoordinator;
                } else if (!coordinator.equals(rowCoordinator)) {
                    coordinatorChanged = true;
                }
            }
            addWorkers(workers);
        }

        String coordinator() {
            return coordinatorChanged ? coordinator + " (changed inside term)" : coordinator;
        }

        private void addWorkers(String workers) {
            if (workers == null || workers.isBlank() || "-".equals(workers)) {
                return;
            }
            for (String part : workers.split(",")) {
                try {
                    int workerId = Integer.parseInt(part.trim());
                    workerCounts.merge(workerId, 1, Integer::sum);
                } catch (NumberFormatException ignored) {
                    // Ignore malformed table text; job execution remains unaffected.
                }
            }
        }
    }

    /** Non-editable table model for submitted tasks. */
    private static final class TaskTableModel extends AbstractTableModel {
        private final List<Object[]> rows = new ArrayList<>();

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public int getColumnCount() {
            return COLUMNS.length;
        }

        @Override
        public String getColumnName(int column) {
            return COLUMNS[column];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return rows.get(rowIndex)[columnIndex];
        }

        @Override
        public boolean isCellEditable(int rowIndex, int columnIndex) {
            return false;
        }

        void addRow(int jobId, String type, String submitted, String status, String coordinator, String workers, String result, String error) {
            rows.add(new Object[]{jobId, termLabelForJob(jobId), type, submitted, status, coordinator, workers, result, error});
            fireTableRowsInserted(rows.size() - 1, rows.size() - 1);
        }

        void updateRow(int jobId, String status, String coordinator, String workers, String result, String error) {
            for (int i = 0; i < rows.size(); i++) {
                if ((Integer) rows.get(i)[0] == jobId) {
                    rows.get(i)[4] = status;
                    rows.get(i)[5] = coordinator;
                    rows.get(i)[6] = workers;
                    rows.get(i)[7] = result;
                    rows.get(i)[8] = error;
                    fireTableRowsUpdated(i, i);
                    return;
                }
            }
        }

        void clear() {
            int lastRow = rows.size() - 1;
            rows.clear();
            if (lastRow >= 0) {
                fireTableRowsDeleted(0, lastRow);
            }
        }

        int doneJobCount() {
            int count = 0;
            for (Object[] row : rows) {
                if ("Done".equals(row[4])) {
                    count++;
                }
            }
            return count;
        }

        Map<Integer, Integer> completedWorkerJobCounts() {
            Map<Integer, Integer> counts = new LinkedHashMap<>();
            for (Object[] row : rows) {
                if (!"Done".equals(row[4])) {
                    continue;
                }
                String workers = String.valueOf(row[6]);
                if (workers.isBlank() || "-".equals(workers)) {
                    continue;
                }
                for (String part : workers.split(",")) {
                    try {
                        int workerId = Integer.parseInt(part.trim());
                        counts.merge(workerId, 1, Integer::sum);
                    } catch (NumberFormatException ignored) {
                        // Ignore malformed table text; job execution remains unaffected.
                    }
                }
            }
            return counts;
        }

        List<TermBreakdown> termBreakdowns() {
            Map<Integer, TermBreakdown> terms = new LinkedHashMap<>();
            for (Object[] row : rows) {
                if (!"Done".equals(row[4])) {
                    continue;
                }
                int jobId = (Integer) row[0];
                int termNumber = termNumberForJob(jobId);
                TermBreakdown term = terms.computeIfAbsent(termNumber, TermBreakdown::new);
                term.addJob(String.valueOf(row[5]), String.valueOf(row[6]));
            }
            return new ArrayList<>(terms.values());
        }
    }
}
