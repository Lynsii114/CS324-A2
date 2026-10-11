package com.cs324.frontend.client;

import com.cs324.backend.api.BootstrapService;
import com.cs324.backend.api.WorkerInfo;
import com.cs324.backend.api.WorkerService;
import com.cs324.backend.bootstrap.BootstrapServer;
import com.cs324.backend.worker.WorkerClusterConfig;
import com.cs324.backend.worker.WorkerServer;
import com.cs324.frontend.ui.UITheme;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client GUI. Runs as its own Java process and talks to the coordinator
 * worker over RMI. Supports manual or CSV input, three job types (MAX,
 * PRIMECOUNT, PRIMESUM) and concurrent submissions from one or more
 * clients at the same time.
 *
 * <p>All RMI calls happen on a small background executor, never on the
 * Swing event thread, so the interface never freezes while a job runs.</p>
 */
public class ClientGUI extends JFrame {

    private static final String[] JOB_TYPES = {"MAX", "PRIMECOUNT", "PRIMESUM"};
    private static final String[] COLUMNS = {"Job ID", "Type", "Submitted", "Status", "Coordinator", "Workers", "Result", "Error"};

    private final JTextField hostField = new JTextField("localhost", 10);
    private final JTextField bootstrapPortField = new JTextField(String.valueOf(BootstrapServer.DEFAULT_PORT), 6);
    private final JButton connectButton = new JButton("Connect");
    private final JLabel connectionStatus = new JLabel("Not connected");

    private final JComboBox<String> jobTypeCombo = new JComboBox<>(JOB_TYPES);
    private final JTextArea inputArea = new JTextArea(4, 40);
    private final JButton loadCsvButton = new JButton("Load CSV...");
    private final JButton submitButton = new JButton("Submit");
    private final JButton clearButton = new JButton("Clear");

    // Dashboard widgets.
    private final JLabel statusPill = new JLabel("  ●  OFFLINE  ");
    private final JLabel coordinatorValue = new JLabel("None");
    private final JLabel workersValue = new JLabel("0/" + WorkerClusterConfig.WORKER_COUNT);
    private final JLabel jobsSubmittedValue = new JLabel("0");
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

    /**
     * This client's identifier. Sent with every submission so the elected
     * coordinator can correlate the distributed task back to the client that
     * asked for it (shown in the client's task table).
     */
    private final String clientId = "client-" + UUID.randomUUID().toString().substring(0, 8);

    private volatile String bootstrapHost = "localhost";
    private volatile int bootstrapPort = BootstrapServer.DEFAULT_PORT;
    private volatile BootstrapService bootstrap;
    private volatile WorkerService coordinator;
    private volatile int activeWorkerCount;
    private volatile int clusterSize = WorkerClusterConfig.WORKER_COUNT;

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ClientGUI().setVisible(true));
    }

    public ClientGUI() {
        super("Distributed Client");
        UITheme.install();
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(960, 760);
        setMinimumSize(new java.awt.Dimension(880, 660));
        setLocationRelativeTo(null);

        getContentPane().setLayout(new BorderLayout());
        getContentPane().setBackground(UITheme.BACKGROUND);
        getContentPane().add(buildDashboard(), BorderLayout.NORTH);
        getContentPane().add(buildMain(), BorderLayout.CENTER);

        style();

        connectButton.addActionListener(e -> connect());
        submitButton.addActionListener(e -> submitJob());
        loadCsvButton.addActionListener(e -> loadCsv());
        clearButton.addActionListener(e -> {
            inputArea.setText("");
            log("Input cleared");
        });
        jobTypeCombo.addActionListener(e -> updateHint());

        updateHint();
        log("Client ready (" + clientId + "). Connect to the Bootstrap Node, then submit jobs "
                + "(they run in the background).");
    }

    private void style() {
        UITheme.button(connectButton);
        UITheme.button(loadCsvButton);
        UITheme.button(clearButton);
        UITheme.button(submitButton);
        UITheme.textField(hostField);
        UITheme.textField(bootstrapPortField);
        UITheme.textArea(inputArea);
        UITheme.textArea(logArea);
        UITheme.combo(jobTypeCombo);
        UITheme.table(taskTable);
        taskTable.setModel(tableModel);
        taskTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        taskTable.setAutoCreateRowSorter(true);
        UITheme.label(connectionStatus, UITheme.MUTED);
        UITheme.label(coordinatorLabel, UITheme.MUTED);
        logArea.setEditable(false);
    }

    // ---------------------------------------------------------------- dashboard

    private JPanel buildDashboard() {
        JPanel dashboard = new JPanel(new GridBagLayout());
        dashboard.setBackground(UITheme.BACKGROUND);

        JPanel connectionCard = card();
        statusPill.setFont(UITheme.BIG);
        statusPill.setForeground(UITheme.ERROR);
        statusPill.setHorizontalAlignment(SwingConstants.CENTER);
        statusPill.setOpaque(true);
        statusPill.setBackground(UITheme.CARD);
        statusPill.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(UITheme.ERROR, 2),
                BorderFactory.createEmptyBorder(12, 24, 12, 24)));
        connectionCard.add(cardTitle("Client Status"), gbc(0, 0, 2));
        connectionCard.add(statusPill, gbc(0, 1, 2));

        JPanel coordinatorCard = card();
        coordinatorValue.setFont(UITheme.BIG);
        coordinatorValue.setForeground(UITheme.TEXT);
        coordinatorValue.setHorizontalAlignment(SwingConstants.CENTER);
        coordinatorCard.add(cardTitle("Coordinator (Elected Node)"), gbc(0, 0, 2));
        coordinatorCard.add(coordinatorValue, gbc(0, 1, 2));

        JPanel statsCard = card();
        workersValue.setFont(UITheme.BIG);
        workersValue.setForeground(UITheme.TEXT);
        workersValue.setHorizontalAlignment(SwingConstants.CENTER);
        jobsSubmittedValue.setFont(UITheme.BIG);
        jobsSubmittedValue.setForeground(UITheme.ACCENT);
        jobsSubmittedValue.setHorizontalAlignment(SwingConstants.CENTER);
        statsCard.add(cardTitle("Workers Online"), gbc(0, 0, 2));
        statsCard.add(workersValue, gbc(0, 1, 2));
        statsCard.add(cardTitle("Jobs Submitted"), gbc(0, 2, 2));
        statsCard.add(jobsSubmittedValue, gbc(0, 3, 2));

        JPanel titleCard = card();
        JLabel title = new JLabel("Distributed Client");
        title.setFont(UITheme.TITLE);
        title.setForeground(UITheme.TEXT);
        JLabel subtitle = new JLabel("MAX  ·  PRIMECOUNT  ·  PRIMESUM   (manual or CSV)");
        subtitle.setFont(UITheme.BASE);
        subtitle.setForeground(UITheme.MUTED);
        titleCard.add(title, gbc(0, 0, 2));
        titleCard.add(subtitle, gbc(0, 1, 2));

        GridBagConstraints g = new GridBagConstraints();
        g.gridy = 0;
        g.weightx = 1;
        g.weighty = 1;
        g.fill = GridBagConstraints.BOTH;
        g.insets = new Insets(8, 8, 8, 8);
        g.gridx = 0;
        dashboard.add(titleCard, g);
        g.gridx = 1;
        dashboard.add(connectionCard, g);
        g.gridx = 2;
        dashboard.add(coordinatorCard, g);
        g.gridx = 3;
        dashboard.add(statsCard, g);
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

    private Component buildMain() {
        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 8));
        connectionPanel.setBackground(UITheme.PANEL);
        connectionPanel.setBorder(UITheme.titledBorder("Connection"));
        JLabel hostLabel = new JLabel("Bootstrap host:");
        UITheme.label(hostLabel, UITheme.MUTED);
        JLabel portLabel = new JLabel("Port:");
        UITheme.label(portLabel, UITheme.MUTED);
        connectionPanel.add(hostLabel);
        connectionPanel.add(hostField);
        connectionPanel.add(portLabel);
        connectionPanel.add(bootstrapPortField);
        connectionPanel.add(connectButton);
        connectionPanel.add(connectionStatus);

        JPanel jobPanel = new JPanel(new BorderLayout(8, 8));
        jobPanel.setBackground(UITheme.PANEL);
        jobPanel.setBorder(UITheme.titledBorder("Job"));

        JPanel jobHeader = new JPanel(new FlowLayout(FlowLayout.LEFT, 12, 8));
        jobHeader.setBackground(UITheme.PANEL);
        JLabel typeLabel = new JLabel("Job type:");
        UITheme.label(typeLabel, UITheme.MUTED);
        jobHeader.add(typeLabel);
        jobHeader.add(jobTypeCombo);
        jobHeader.add(coordinatorLabel);

        JPanel jobButtons = new JPanel(new FlowLayout(FlowLayout.RIGHT, 10, 8));
        jobButtons.setBackground(UITheme.PANEL);
        jobButtons.add(loadCsvButton);
        jobButtons.add(clearButton);
        jobButtons.add(submitButton);

        JPanel inputPanel = new JPanel(new BorderLayout());
        inputPanel.setBackground(UITheme.PANEL);
        JScrollPane inputScroll = new JScrollPane(inputArea);
        UITheme.scroll(inputScroll);
        inputPanel.add(inputScroll, BorderLayout.CENTER);

        jobPanel.add(jobHeader, BorderLayout.NORTH);
        jobPanel.add(inputPanel, BorderLayout.CENTER);
        jobPanel.add(jobButtons, BorderLayout.SOUTH);

        JPanel top = new JPanel(new BorderLayout(8, 8));
        top.setBackground(UITheme.BACKGROUND);
        top.add(connectionPanel, BorderLayout.NORTH);
        top.add(jobPanel, BorderLayout.CENTER);

        JPanel tablePanel = new JPanel(new BorderLayout());
        tablePanel.setBackground(UITheme.PANEL);
        tablePanel.setBorder(UITheme.titledBorder("Tasks (concurrent submissions supported)"));
        JScrollPane tableScroll = new JScrollPane(taskTable);
        UITheme.scroll(tableScroll);
        tablePanel.add(tableScroll, BorderLayout.CENTER);

        JPanel logPanel = new JPanel(new BorderLayout());
        logPanel.setBackground(UITheme.PANEL);
        logPanel.setBorder(UITheme.titledBorder("Log"));
        JScrollPane logScroll = new JScrollPane(logArea);
        UITheme.scroll(logScroll);
        logPanel.add(logScroll, BorderLayout.CENTER);

        JSplitPane bottom = new JSplitPane(JSplitPane.VERTICAL_SPLIT, tablePanel, logPanel);
        bottom.setResizeWeight(0.6);
        bottom.setBackground(UITheme.BACKGROUND);

        JSplitPane main = new JSplitPane(JSplitPane.VERTICAL_SPLIT, top, bottom);
        main.setResizeWeight(0.38);
        main.setBackground(UITheme.BACKGROUND);
        return main;
    }

    private void updateHint() {
        String type = (String) jobTypeCombo.getSelectedItem();
        inputArea.setToolTipText("PRIMESUM".equals(type) ? "start,end  e.g. 1,1000"
                : "comma-separated integers  e.g. 12,5,99,2,17");
    }

    // ---------------------------------------------------------------- connection

    private void connect() {
        bootstrapHost = hostField.getText().trim().isEmpty() ? "localhost" : hostField.getText().trim();
        try {
            bootstrapPort = Integer.parseInt(bootstrapPortField.getText().trim());
        } catch (NumberFormatException e) {
            appendError("Bootstrap port must be a number");
            return;
        }

        connectButton.setEnabled(false);
        connectionStatus.setForeground(UITheme.WARNING);
        connectionStatus.setText("Connecting...");
        jobExecutor.submit(this::doConnect);
    }

    private void doConnect() {
        try {
            Registry registry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
            BootstrapService foundBootstrap = (BootstrapService) registry.lookup(BootstrapServer.SERVICE_NAME);
            bootstrap = foundBootstrap;
            List<WorkerInfo> active = foundBootstrap.getActiveWorkers();
            activeWorkerCount = active.size();
            try {
                clusterSize = foundBootstrap.getClusterSize();
            } catch (Exception e) {
                clusterSize = Math.max(activeWorkerCount, WorkerClusterConfig.WORKER_COUNT);
            }
            int coordinatorId = findCoordinatorId(foundBootstrap);
            WorkerService found = coordinatorId == WorkerService.NO_COORDINATOR
                    ? null
                    : lookupRegistered(foundBootstrap, coordinatorId);
            coordinator = found;
            final int elected = coordinatorId;
            SwingUtilities.invokeLater(() -> {
                connectButton.setEnabled(true);
                workersValue.setText(activeWorkerCount + "/" + clusterSize);
                if (elected == WorkerService.NO_COORDINATOR) {
                    connectionStatus.setForeground(UITheme.WARNING);
                    connectionStatus.setText("Connected - no coordinator yet");
                    coordinatorLabel.setText("Coordinator: none (workers are electing...)");
                    coordinatorValue.setText("None");
                    coordinatorValue.setForeground(UITheme.WARNING);
                    statusPill.setText("  ●  ELECTING  ");
                    statusPill.setForeground(UITheme.WARNING);
                    statusPill.setBorder(BorderFactory.createCompoundBorder(
                            BorderFactory.createLineBorder(UITheme.WARNING, 2),
                            BorderFactory.createEmptyBorder(12, 24, 12, 24)));
                } else {
                    connectionStatus.setForeground(UITheme.SUCCESS);
                    connectionStatus.setText("Online - coordinator worker " + elected);
                    coordinatorLabel.setText("Coordinator: worker " + elected);
                    coordinatorValue.setText("Worker " + elected);
                    coordinatorValue.setForeground(UITheme.SUCCESS);
                    statusPill.setText("  ●  ONLINE  ");
                    statusPill.setForeground(UITheme.SUCCESS);
                    statusPill.setBorder(BorderFactory.createCompoundBorder(
                            BorderFactory.createLineBorder(UITheme.SUCCESS, 2),
                            BorderFactory.createEmptyBorder(12, 24, 12, 24)));
                }
                log("Connected to Bootstrap Node at " + bootstrapHost + ":" + bootstrapPort
                        + " - " + activeWorkerCount + " worker(s) registered"
                        + (elected == WorkerService.NO_COORDINATOR ? ", no coordinator elected yet" : ", coordinator is worker " + elected));
            });
        } catch (Exception e) {
            SwingUtilities.invokeLater(() -> {
                connectButton.setEnabled(true);
                connectionStatus.setForeground(UITheme.ERROR);
                connectionStatus.setText("Connection failed");
                appendError("Could not connect: " + rootMessage(e));
            });
        }
    }

    /**
     * Asks every registered worker which coordinator it knows and returns the
     * agreed coordinator id (or {@link WorkerService#NO_COORDINATOR} if none).
     */
    private int findCoordinatorId(BootstrapService bootstrap) throws Exception {
        for (WorkerInfo info : bootstrap.getActiveWorkers()) {
            WorkerService worker = lookup(info);
            if (worker == null) {
                continue;
            }
            int candidate = worker.getCurrentCoordinatorId();
            if (candidate != WorkerService.NO_COORDINATOR) {
                return candidate;
            }
        }
        return WorkerService.NO_COORDINATOR;
    }

    private WorkerService lookupRegistered(BootstrapService bootstrap, int workerId) throws Exception {
        for (WorkerInfo info : bootstrap.getActiveWorkers()) {
            if (info.getWorkerId() == workerId) {
                WorkerService worker = lookup(info);
                if (worker != null) {
                    return worker;
                }
            }
        }
        throw new Exception("Coordinator worker " + workerId + " is not reachable");
    }

    private WorkerService lookup(WorkerInfo info) {
        try {
            Registry registry = LocateRegistry.getRegistry(info.getHost(), info.getPort());
            return (WorkerService) registry.lookup(WorkerServer.SERVICE_NAME_PREFIX + info.getWorkerId());
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
        final BootstrapService currentBootstrap = bootstrap;
        if (currentBootstrap == null) {
            return false;
        }
        try {
            int coordinatorId = findCoordinatorId(currentBootstrap);
            WorkerService found = coordinatorId == WorkerService.NO_COORDINATOR
                    ? null
                    : lookupRegistered(currentBootstrap, coordinatorId);
            coordinator = found;
            final int elected = coordinatorId;
            SwingUtilities.invokeLater(() -> {
                if (elected == WorkerService.NO_COORDINATOR) {
                    coordinatorLabel.setText("Coordinator: none (workers are electing...)");
                    coordinatorValue.setText("None");
                    coordinatorValue.setForeground(UITheme.WARNING);
                    statusPill.setText("  ●  ELECTING  ");
                    statusPill.setForeground(UITheme.WARNING);
                } else {
                    coordinatorLabel.setText("Coordinator: worker " + elected);
                    coordinatorValue.setText("Worker " + elected);
                    coordinatorValue.setForeground(UITheme.SUCCESS);
                    statusPill.setText("  ●  ONLINE  ");
                    statusPill.setForeground(UITheme.SUCCESS);
                }
            });
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ---------------------------------------------------------------- jobs

    private void submitJob() {
        if (coordinator == null) {
            appendError("No coordinator - press Connect (workers elect one automatically)");
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
        final WorkerService target = coordinator;

        addTask(jobId, type, submitted, "Running", String.valueOf(currentCoordinatorId(target)), "-", "-", null);
        jobsSubmittedValue.setText(String.valueOf(jobSequence.get()));

        jobExecutor.submit(() -> {
            WorkerService targetWorker = target;
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    com.cs324.backend.api.TaskResult result = dispatch(targetWorker, type, raw);
                    String workers = joinWorkerIds(result.getWorkerIds());
                    String resultText = "task=" + shortTaskId(result.getTaskId()) + " result=" + result.getResult();
                    updateTask(jobId, "Done", workers, resultText, null);
                    log("Job " + jobId + " [" + type + "] finished -> " + result);
                    return;
                } catch (Exception e) {
                    String message = rootMessage(e);
                    boolean coordinatorChanged = attempt < 2 && needsNewCoordinator(message);
                    if (coordinatorChanged && refreshCoordinator() && coordinator != null) {
                        log("Term hand-over detected - re-resolving the new coordinator and retrying job " + jobId);
                        targetWorker = coordinator;
                        try {
                            Thread.sleep(1000L);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        continue;
                    }
                    updateTask(jobId, "Failed", "-", "-",
                            coordinatorChanged ? message + " (coordinator re-elected)" : message);
                    log("Job " + jobId + " [" + type + "] failed -> " + message);
                    return;
                }
            }
        });
    }

    private int currentCoordinatorId(WorkerService worker) {
        try {
            return worker.getCurrentCoordinatorId();
        } catch (Exception e) {
            return WorkerService.NO_COORDINATOR;
        }
    }

    /** True when the failure means the elected coordinator changed (term ended or not the coordinator). */
    private boolean needsNewCoordinator(String message) {
        return message != null && (message.contains("not the coordinator")
                || message.contains("Coordinator term ended"));
    }

    private com.cs324.backend.api.TaskResult dispatch(WorkerService target, String type, String raw) throws Exception {
        switch (type) {
            case "MAX":
                return target.submitMaxJob(clientId, parseNumbers(raw));
            case "PRIMECOUNT":
                return target.submitPrimeCount(clientId, parseNumbers(raw));
            case "PRIMESUM":
                int[] range = parseRange(raw);
                return target.submitPrimeSum(clientId, range[0], range[1]);
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
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        Path path = chooser.getSelectedFile().toPath();
        try {
            String content = Files.readString(path).trim();
            inputArea.setText(content);
            log("Loaded CSV from " + path.getFileName());
        } catch (IOException e) {
            appendError("Could not read CSV file: " + e.getMessage());
        }
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
        SwingUtilities.invokeLater(() -> tableModel.addRow(jobId, type, submitted, status, coordinator, workers, result, error));
    }

    private void updateTask(int jobId, String status, String workers, String result, String error) {
        SwingUtilities.invokeLater(() -> tableModel.updateRow(jobId, status, workers, result, error));
    }

    private void log(String message) {
        logArea.append("[" + LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")) + "] " + message + "\n");
        logArea.setCaretPosition(logArea.getDocument().getLength());
    }

    private void appendError(String message) {
        log("ERROR: " + message);
        JOptionPane.showMessageDialog(this, message, "Error", JOptionPane.ERROR_MESSAGE);
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
            rows.add(new Object[]{jobId, type, submitted, status, coordinator, workers, result, error});
            fireTableRowsInserted(rows.size() - 1, rows.size() - 1);
        }

        void updateRow(int jobId, String status, String workers, String result, String error) {
            for (int i = 0; i < rows.size(); i++) {
                if ((Integer) rows.get(i)[0] == jobId) {
                    rows.get(i)[3] = status;
                    rows.get(i)[5] = workers;
                    rows.get(i)[6] = result;
                    rows.get(i)[7] = error;
                    fireTableRowsUpdated(i, i);
                    return;
                }
            }
        }
    }
}