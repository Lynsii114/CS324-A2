package com.cs324.backend.gui;

import com.cs324.backend.api.TaskResult;
import com.cs324.backend.api.WorkerService;
import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.bootstrap.BootstrapServiceImpl;
import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerClusterLauncher;
import com.cs324.backend.worker.WorkerServer;
import com.cs324.frontend.ui.UITheme;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.table.AbstractTableModel;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Server-side GUI. From here the Bootstrap Node and the six worker processes
 * are started and stopped. The workers elect a coordinator automatically in the
 * background, so the operator just watches the dashboard: network online / offline,
 * which worker was elected, and the live per-worker state table.
 *
 * <p>Heavy actions (bootstrap start, worker launch) run on a background
 * executor; the UI is only touched through {@code invokeLater}.</p>
 */
public class ServerGUI extends JFrame {

    private static final String[] COLUMNS = {"Worker", "Priority", "Port", "Online", "JAC", "JobsThisTerm", "Coordinator", "Neighbours", "Leaderman"};
    private static final String[] TASK_COLUMNS = {"Time", "Task", "Client", "Job Type", "Segments", "Workers Used", "Result"};
    private static final int MAX_TASK_ROWS = 60;

    private final JTextField hostField = new JTextField(WorkerClusterConfig.DEFAULT_HOST, 10);
    private final JTextField bootstrapPortField = new JTextField(String.valueOf(BootstrapServer.DEFAULT_PORT), 6);
    private final JLabel bootstrapStatus = new JLabel("Bootstrap: not started");

    private final JButton startBootstrapButton = new JButton("Start Bootstrap");
    private final JButton startWorkersButton = new JButton("Start Workers");
    private final JButton stopWorkersButton = new JButton("Stop Workers");
    private final JButton refreshButton = new JButton("Refresh");

    // Dashboard widgets.
    private final JLabel networkPill = new JLabel("  ●  OFFLINE  ");
    private final JLabel networkDetail = new JLabel("Workers online: 0/" + WorkerClusterConfig.WORKER_COUNT);
    private final JLabel coordinatorValue = new JLabel("None");
    private final JLabel electionValue = new JLabel("Waiting for workers...");

    private final JTextArea logArea = new JTextArea(10, 60);
    private final JTable statusTable = new JTable();
    private final StatusTableModel tableModel = new StatusTableModel();
    private final JTable taskTable = new JTable();
    private final TaskDistributionModel taskModel = new TaskDistributionModel();

    private final ExecutorService executor = Executors.newFixedThreadPool(2, runnable -> {
        Thread thread = new Thread(runnable, "server-gui-worker");
        thread.setDaemon(true);
        return thread;
    });

    private volatile String bootstrapHost = WorkerClusterConfig.DEFAULT_HOST;
    private volatile int bootstrapPort = BootstrapServer.DEFAULT_PORT;
    private int previousCoordinator = WorkerService.NO_COORDINATOR;

    /**
     * Workers launched by this Server Manager session. When the GUI closes, only
     * these are stopped - so closing the manager never leaves orphaned worker
     * processes behind (the cause of "workers show online on a fresh start"),
     * and workers started manually from a terminal are left untouched.
     */
    private final Set<Integer> sessionLaunchedWorkers = ConcurrentHashMap.newKeySet();

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ServerGUI().setVisible(true));
    }

    public ServerGUI() {
        super("Server Manager");
        UITheme.install();
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(960, 760);
        setMinimumSize(new java.awt.Dimension(880, 660));
        setLocationRelativeTo(null);

        style();

        getContentPane().setLayout(new BorderLayout());
        getContentPane().add(buildDashboard(), BorderLayout.NORTH);
        getContentPane().add(buildMain(), BorderLayout.CENTER);

        startBootstrapButton.addActionListener(e -> startBootstrap());
        startWorkersButton.addActionListener(e -> startWorkers());
        stopWorkersButton.addActionListener(e -> stopWorkers());
        refreshButton.addActionListener(e -> refreshStatus());

        log("Server manager ready. Start the Bootstrap Node, then start the workers - "
                + "they elect a coordinator automatically (lowest JAC wins, ties go to the "
                + "highest worker ID), no manual trigger needed.");

        // Closing the Server Manager must not leave the six worker JVMs running in the
        // background: orphaned workers keep answering on their RMI ports, so the next
        // launch of this GUI would show them "Online" although nobody started them this
        // session. On exit we only stop the workers this session launched.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!sessionLaunchedWorkers.isEmpty()) {
                System.out.println("[ServerGUI] closing - stopping workers launched this session: "
                        + sessionLaunchedWorkers);
                WorkerClusterLauncher.stop(new ArrayList<>(sessionLaunchedWorkers));
            }
        }, "server-gui-shutdown"));

        executor.submit(() -> {
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            checkForLeftoverWorkers();
        });

        Timer timer = new Timer(2500, e -> refreshBackground());
        timer.start();
    }

    private void style() {
        getContentPane().setBackground(UITheme.BACKGROUND);
        UITheme.label(bootstrapStatus, UITheme.MUTED);
        for (JButton button : new JButton[]{startBootstrapButton, startWorkersButton,
                stopWorkersButton, refreshButton}) {
            UITheme.button(button);
        }
        UITheme.textField(hostField);
        UITheme.textField(bootstrapPortField);
        UITheme.textArea(logArea);
        UITheme.table(statusTable);
        statusTable.setModel(tableModel);
        statusTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        UITheme.table(taskTable);
        taskTable.setModel(taskModel);
        taskTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    }

    // ---------------------------------------------------------------- dashboard

    private JPanel buildDashboard() {
        JPanel dashboard = new JPanel(new GridBagLayout());
        dashboard.setBackground(UITheme.BACKGROUND);

        JPanel networkCard = card();
        networkPill.setFont(UITheme.BIG);
        networkPill.setForeground(UITheme.ERROR);
        networkPill.setHorizontalAlignment(SwingConstants.CENTER);
        networkPill.setBackground(UITheme.CARD);
        networkPill.setOpaque(true);
        networkPill.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UITheme.ERROR, 2),
                BorderFactory.createEmptyBorder(12, 24, 12, 24)));
        networkDetail.setFont(UITheme.MEDIUM);
        networkDetail.setForeground(UITheme.MUTED);
        networkDetail.setHorizontalAlignment(SwingConstants.CENTER);
        networkCard.add(networkPill, gbc(0, 0, 2));
        networkCard.add(networkDetail, gbc(0, 1, 2));

        JPanel coordinatorCard = card();
        coordinatorValue.setFont(UITheme.BIG);
        coordinatorValue.setForeground(UITheme.TEXT);
        coordinatorValue.setHorizontalAlignment(SwingConstants.CENTER);
        coordinatorCard.add(cardTitle("Elected Coordinator"), gbc(0, 0, 2));
        coordinatorCard.add(coordinatorValue, gbc(0, 1, 2));

        JPanel electionCard = card();
        electionValue.setFont(UITheme.BIG);
        electionValue.setForeground(UITheme.WARNING);
        electionValue.setHorizontalAlignment(SwingConstants.CENTER);
        electionCard.add(cardTitle("Election"), gbc(0, 0, 2));
        electionCard.add(electionValue, gbc(0, 1, 2));

        JPanel titleCard = card();
        JLabel title = new JLabel("Cluster Server Manager");
        title.setFont(UITheme.TITLE);
        title.setForeground(UITheme.TEXT);
        JLabel subtitle = new JLabel("Bootstrap  ·  " + WorkerClusterConfig.WORKER_COUNT + " Workers  ·  auto election  ·  JAC-based scheduling  ·  RMI");
        subtitle.setFont(UITheme.BASE);
        subtitle.setForeground(UITheme.MUTED);
        titleCard.add(title, gbc(0, 0, 2));
        titleCard.add(subtitle, gbc(0, 1, 2));

        GridBagConstraints g = new GridBagConstraints();
        g.gridx = 0;
        g.gridy = 0;
        g.weightx = 1;
        g.weighty = 1;
        g.fill = GridBagConstraints.BOTH;
        g.insets = new Insets(8, 8, 8, 8);
        dashboard.add(titleCard, g);
        g.gridx = 1;
        dashboard.add(networkCard, g);
        g.gridx = 2;
        dashboard.add(coordinatorCard, g);
        g.gridx = 3;
        dashboard.add(electionCard, g);
        return dashboard;
    }

    private static JPanel card() {
        JPanel panel = new JPanel(new GridBagLayout());
        panel.setBackground(UITheme.CARD);
        panel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UITheme.BORDER),
                BorderFactory.createEmptyBorder(10, 10, 10, 10)));
        return panel;
    }

    private static JLabel cardTitle(String text) {
        JLabel label = new JLabel(text);
        label.setFont(UITheme.SECTION);
        label.setForeground(UITheme.MUTED);
        label.setHorizontalAlignment(SwingConstants.CENTER);
        return label;
    }

    private static GridBagConstraints gbc(int x, int y, int width) {
        GridBagConstraints c = new GridBagConstraints();
        c.gridx = x;
        c.gridy = y;
        c.gridwidth = width;
        c.insets = new Insets(4, 4, 4, 4);
        return c;
    }

    // ---------------------------------------------------------------- panels

    private JPanel buildControlPanel() {
        JPanel panel = new JPanel(new BorderLayout());
        UITheme.panel(panel);
        panel.setBorder(UITheme.titledBorder("Cluster Management"));

        JPanel settings = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 8));
        settings.setBackground(UITheme.PANEL);
        JLabel hostLabel = new JLabel("Host:");
        UITheme.label(hostLabel, UITheme.MUTED);
        JLabel portLabel = new JLabel("Bootstrap port:");
        UITheme.label(portLabel, UITheme.MUTED);
        settings.add(hostLabel);
        settings.add(hostField);
        settings.add(portLabel);
        settings.add(bootstrapPortField);

        JPanel actions = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 8));
        actions.setBackground(UITheme.PANEL);
        actions.add(startBootstrapButton);
        actions.add(startWorkersButton);
        actions.add(stopWorkersButton);
        actions.add(refreshButton);

        panel.add(settings, BorderLayout.NORTH);
        panel.add(actions, BorderLayout.CENTER);
        panel.add(bootstrapStatus, BorderLayout.SOUTH);
        return panel;
    }

    private Component buildMain() {
        JPanel statusPanel = new JPanel(new BorderLayout());
        statusPanel.setBackground(UITheme.PANEL);
        JScrollPane statusScroll = new JScrollPane(statusTable);
        UITheme.scroll(statusScroll);
        statusPanel.add(statusScroll, BorderLayout.CENTER);

        JPanel taskPanel = new JPanel(new BorderLayout());
        taskPanel.setBackground(UITheme.PANEL);
        JScrollPane taskScroll = new JScrollPane(taskTable);
        UITheme.scroll(taskScroll);
        taskPanel.add(taskScroll, BorderLayout.CENTER);

        JTabbedPane tabs = new JTabbedPane();
        tabs.setFont(UITheme.BASE);
        tabs.setBackground(UITheme.BACKGROUND);
        tabs.addTab("Worker Status", statusPanel);
        tabs.addTab("Client Task Distribution", taskPanel);

        JPanel logPanel = new JPanel(new BorderLayout());
        logPanel.setBackground(UITheme.PANEL);
        JScrollPane logScroll = new JScrollPane(logArea);
        UITheme.scroll(logScroll);
        logPanel.add(logScroll, BorderLayout.CENTER);

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tabs, logPanel);
        split.setResizeWeight(0.62);
        split.setBackground(UITheme.BACKGROUND);
        split.setBorder(UITheme.titledBorder("Cluster State (auto-refreshes)"));

        JPanel wrapper = new JPanel(new BorderLayout());
        wrapper.setBackground(UITheme.BACKGROUND);
        wrapper.add(buildControlPanel(), BorderLayout.NORTH);
        wrapper.add(split, BorderLayout.CENTER);
        return wrapper;
    }

    // ---------------------------------------------------------------- actions

    private void startBootstrap() {
        readSettings();
        startBootstrapButton.setEnabled(false);
        executor.submit(() -> {
            try {
                Registry registry;
                try {
                    registry = LocateRegistry.createRegistry(bootstrapPort);
                } catch (ExportException alreadyRunning) {
                    registry = LocateRegistry.getRegistry(bootstrapPort);
                }
                try {
                    registry.lookup(BootstrapServer.SERVICE_NAME);
                    log("Bootstrap Node already running on port " + bootstrapPort);
                } catch (Exception notBound) {
                    registry.rebind(BootstrapServer.SERVICE_NAME, new BootstrapServiceImpl());
                    log("Bootstrap Node started and bound on port " + bootstrapPort);
                }
                SwingUtilities.invokeLater(() -> {
                    bootstrapStatus.setForeground(UITheme.SUCCESS);
                    bootstrapStatus.setText("Bootstrap: running on " + bootstrapHost + ":" + bootstrapPort);
                    startBootstrapButton.setEnabled(true);
                    refreshBackground();
                });
            } catch (Exception e) {
                SwingUtilities.invokeLater(() -> {
                    startBootstrapButton.setEnabled(true);
                    appendError("Bootstrap Node failed to start: " + rootMessage(e));
                });
            }
        });
    }

    private void startWorkers() {
        readSettings();
        startWorkersButton.setEnabled(false);
        executor.submit(() -> {
            boolean started = false;
            try {
                log("Launching " + WorkerClusterConfig.WORKER_COUNT + " worker processes (this can take a few seconds)...");
                List<Integer> launched = WorkerClusterLauncher.start(bootstrapHost, bootstrapPort);
                sessionLaunchedWorkers.addAll(launched);
                started = !launched.isEmpty();
            } catch (Exception e) {
                log("Could not start workers: " + rootMessage(e));
            }
            final boolean ok = started;
            SwingUtilities.invokeLater(() -> {
                startWorkersButton.setEnabled(true);
                if (ok) {
                    log("Workers launched - they elect a coordinator automatically in the background.");
                }
                refreshBackground();
            });
        });
    }

    private void stopWorkers() {
        executor.submit(() -> {
            WorkerClusterLauncher.stop();
            sessionLaunchedWorkers.clear();
            log("Worker processes stopped - the dashboard will show OFFLINE");
            refreshBackground();
        });
    }

    /**
     * Workers should show OFFLINE until this session starts them. If RMI probing
     * still finds workers online, they were left behind by an earlier run (or
     * started manually) and are not tracked by this manager - surface that once,
     * so the dashboard does not look like a fresh offline cluster.
     */
    private void checkForLeftoverWorkers() {
        if (!sessionLaunchedWorkers.isEmpty()) {
            return;
        }
        int leftover = 0;
        for (int workerId : WorkerClusterConfig.workerIds()) {
            boolean reachable;
            try {
                lookupWorker(workerId);
                reachable = true;
            } catch (Exception e) {
                reachable = false;
            }
            if (reachable && !WorkerClusterLauncher.isManagedAndAlive(workerId)) {
                leftover++;
            }
        }
        if (leftover > 0) {
            log("WARNING: " + leftover + "/" + WorkerClusterConfig.WORKER_COUNT
                    + " worker process(es) are already online but were not started from this session"
                    + " (left over from an earlier run, or launched manually). They occupy ports "
                    + WorkerClusterConfig.BASE_WORKER_PORT + ".."
                    + WorkerClusterConfig.portFor(WorkerClusterConfig.LAST_WORKER_ID)
                    + ". Click 'Stop Workers' to reclaim them, then 'Start Workers'.");
        }
    }

    private void refreshStatus() {
        executor.submit(this::refreshBackground);
    }

    // ---------------------------------------------------------------- status collection

    private void refreshBackground() {
        List<Object[]> rows = new ArrayList<>();
        int reachable = 0;
        Set<Integer> coordinators = new TreeSet<>();

        for (int workerId : WorkerClusterConfig.workerIds()) {
            try {
                WorkerService worker = lookupWorker(workerId);
                int coordinator = worker.getCurrentCoordinatorId();
                coordinators.add(coordinator);
                rows.add(new Object[]{
                        worker.getWorkerId(),
                        priorityText(worker.getPriorityId()),
                        WorkerClusterConfig.portFor(workerId),
                        "yes",
                        worker.getJobAllocationCounter(),
                        worker.getJobsThisTerm(),
                        coordinator == WorkerService.NO_COORDINATOR ? "none" : coordinator,
                        worker.getNeighbours().size(),
                        worker.getLeaderman()});
                reachable++;
            } catch (Exception e) {
                rows.add(new Object[]{workerId, priorityText(0), WorkerClusterConfig.portFor(workerId),
                        "no", "-", "-", "-", "-", "-"});
            }
        }

        final int reachableCount = reachable;
        final int total = WorkerClusterConfig.WORKER_COUNT;
        final boolean agreed = coordinators.size() == 1 && !coordinators.contains(WorkerService.NO_COORDINATOR);
        final int elected = agreed ? coordinators.iterator().next() : WorkerService.NO_COORDINATOR;

        List<TaskResult> mergedTasks = new ArrayList<>();
        for (int workerId : WorkerClusterConfig.workerIds()) {
            try {
                mergedTasks.addAll(lookupWorker(workerId).getTaskHistory());
            } catch (Exception e) {
                // worker unreachable - skip its (unavailable) history
            }
        }
        mergedTasks.sort((a, b) -> Long.compare(b.getCompletedAtMillis(), a.getCompletedAtMillis()));
        if (mergedTasks.size() > MAX_TASK_ROWS) {
            mergedTasks = new ArrayList<>(mergedTasks.subList(0, MAX_TASK_ROWS));
        }
        List<Object[]> taskRows = new ArrayList<>();
        for (TaskResult task : mergedTasks) {
            taskRows.add(new Object[]{
                    LocalTime.ofNanoOfDay(task.getCompletedAtMillis() % (24L * 60 * 60 * 1000) * 1000_000L)
                            .format(DateTimeFormatter.ofPattern("HH:mm:ss")),
                    shortTaskId(task.getTaskId()),
                    task.getClientId(),
                    task.getJobType(),
                    task.getSegmentCount(),
                    joinWorkerIds(task.getWorkerIds()) + " (of " + task.getCoordinatorId() + ")",
                    task.getResult()});
        }

        SwingUtilities.invokeLater(() -> {
            tableModel.setRows(rows);
            taskModel.setRows(taskRows);
            updateDashboard(reachableCount, total, agreed, elected,
                    coordinators.contains(WorkerService.NO_COORDINATOR));
            if (elected != previousCoordinator) {
                if (elected != WorkerService.NO_COORDINATOR) {
                    log("Election complete -> coordinator is worker " + elected);
                }
                previousCoordinator = elected;
            }
        });
    }

    private void updateDashboard(int reachable, int total, boolean agreed, int elected, boolean anyWithoutCoordinator) {
        if (reachable == 0) {
            networkPill.setText("  ●  OFFLINE  ");
            networkPill.setForeground(UITheme.ERROR);
            networkPill.setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(UITheme.ERROR, 2),
                    BorderFactory.createEmptyBorder(12, 24, 12, 24)));
            electionValue.setText("No workers");
            electionValue.setForeground(UITheme.MUTED);
        } else if (reachable == total) {
            networkPill.setText("  ●  ONLINE  ");
            networkPill.setForeground(UITheme.SUCCESS);
            networkPill.setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(UITheme.SUCCESS, 2),
                    BorderFactory.createEmptyBorder(12, 24, 12, 24)));
        } else {
            networkPill.setText("  ●  PARTIAL  ");
            networkPill.setForeground(UITheme.WARNING);
            networkPill.setBorder(BorderFactory.createCompoundBorder(
                    BorderFactory.createLineBorder(UITheme.WARNING, 2),
                    BorderFactory.createEmptyBorder(12, 24, 12, 24)));
        }
        networkDetail.setText("Workers online: " + reachable + "/" + total);

        if (agreed) {
            coordinatorValue.setText("Worker " + elected);
            coordinatorValue.setForeground(UITheme.SUCCESS);
            electionValue.setText("COMPLETE");
            electionValue.setForeground(UITheme.SUCCESS);
        } else if (reachable > 0 && anyWithoutCoordinator) {
            coordinatorValue.setText("None");
            coordinatorValue.setForeground(UITheme.MUTED);
            electionValue.setText(reachable == total ? "ELECTING..." : "Partial network");
            electionValue.setForeground(UITheme.WARNING);
        } else {
            coordinatorValue.setText("Mismatch");
            coordinatorValue.setForeground(UITheme.ERROR);
            electionValue.setText("DIVERGED");
            electionValue.setForeground(UITheme.ERROR);
        }
    }

    private void readSettings() {
        String host = hostField.getText().trim();
        bootstrapHost = host.isEmpty() ? WorkerClusterConfig.DEFAULT_HOST : host;
        try {
            bootstrapPort = Integer.parseInt(bootstrapPortField.getText().trim());
        } catch (NumberFormatException e) {
            appendError("Bootstrap port must be a number");
        }
    }

    /** Startup-lottery priority (1..6), or a placeholder while the lottery runs. */
    private static String priorityText(int priorityId) {
        return priorityId > 0 ? String.valueOf(priorityId) : "-";
    }

    private WorkerService lookupWorker(int workerId) throws Exception {
        Registry registry = LocateRegistry.getRegistry(bootstrapHost, WorkerClusterConfig.portFor(workerId));
        return (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + workerId);
    }

    private String rootMessage(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null ? current.getClass().getSimpleName() : message;
    }

    private void log(String message) {
        logArea.append("[" + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] " + message + "\n");
        logArea.setCaretPosition(logArea.getDocument().getLength());
    }

    private void appendError(String message) {
        log("ERROR: " + message);
        JOptionPane.showMessageDialog(this, message, "Error", JOptionPane.ERROR_MESSAGE);
    }

    private static String shortTaskId(String taskId) {
        return taskId != null && taskId.length() > 8 ? taskId.substring(0, 8) : String.valueOf(taskId);
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

    private static final class StatusTableModel extends AbstractTableModel {
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

        void setRows(List<Object[]> rows) {
            this.rows.clear();
            this.rows.addAll(rows);
            fireTableDataChanged();
        }
    }

    private static final class TaskDistributionModel extends AbstractTableModel {
        private final List<Object[]> rows = new ArrayList<>();

        @Override
        public int getRowCount() {
            return rows.size();
        }

        @Override
        public int getColumnCount() {
            return TASK_COLUMNS.length;
        }

        @Override
        public String getColumnName(int column) {
            return TASK_COLUMNS[column];
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex) {
            return rows.get(rowIndex)[columnIndex];
        }

        void setRows(List<Object[]> rows) {
            this.rows.clear();
            this.rows.addAll(rows);
            fireTableDataChanged();
        }
    }
}