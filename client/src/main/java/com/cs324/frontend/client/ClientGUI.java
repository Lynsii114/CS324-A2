package com.cs324.frontend.client;

import com.cs324.common.GrpcChannels;
import com.cs324.common.ClusterConfig;
import com.cs324.frontend.ui.UITheme;
import com.cs324.proto.BootstrapServiceGrpc;
import com.cs324.proto.ClientServiceGrpc;
import com.cs324.proto.Empty;
import com.cs324.proto.JobRequest;
import com.cs324.proto.JobType;
import com.cs324.proto.TaskStatus;
import com.cs324.proto.TaskUpdate;
import com.cs324.proto.WorkerInfo;
import com.cs324.proto.WorkerProgress;
import io.grpc.StatusRuntimeException;

import atlantafx.base.theme.Styles;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.scene.control.Button;
import javafx.scene.control.ButtonType;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ListCell;
import javafx.scene.control.ListView;
import javafx.scene.control.ProgressBar;
import javafx.scene.control.SelectionMode;
import javafx.scene.control.SplitPane;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.control.Tooltip;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.Priority;
import javafx.scene.layout.Region;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Distributed Cluster client - a JavaFX GUI combining JavaFX controls with the
 * AtlantaFX theme for a modern look and feel.
 *
 * <p>The dashboard reports the live health of the system:</p>
 * <ul>
 *   <li><b>Cluster Server</b> card - RUNNING / FAILURE DETECTED / RECONNECTING /
 *       RECOVERED / OFFLINE, driven by {@link ClusterMonitor} probes;</li>
 *   <li><b>Coordinator</b> card - ONLINE / FAILURE DETECTED / RECOVERED with
 *       failover events;</li>
 *   <li><b>Workers Online</b> and <b>Jobs</b> cards;</li>
 *   <li><b>Event timeline</b> - every server failure, reconnect attempt,
 *       recovery and coordinator event, timestamped.</li>
 * </ul>
 *
 * <p>Jobs ({@code MAX}, {@code PRIMECOUNT}, {@code PRIMESUM}) stream progress
 * from the coordinator over gRPC; interrupted streams are automatically
 * re-resolved and resumed from the Bootstrap Node's task ledger.</p>
 */
public class ClientGUI implements ClusterMonitor.Listener {

    private static final String[] JOB_TYPES = {"MAX", "PRIMECOUNT", "PRIMESUM"};
    private static final int MAX_RESUME_ATTEMPTS = 5;
    private static final long RESOLVE_WAIT_MS = 2_000;
    private static final long STREAM_DEADLINE_SECONDS = 15;
    private static final DateTimeFormatter CLOCK = DateTimeFormatter.ofPattern("HH:mm:ss");

    // ---------------------------------------------------------------- widgets
    private final TextField hostField = new TextField("localhost");
    private final TextField bootstrapPortField =
            new TextField(String.valueOf(EndpointDefaults.BOOTSTRAP_PORT));
    private final Button connectButton = new Button("Connect");
    private final Label connectionStatus = new Label("Not connected");

    private final ComboBox<String> jobTypeCombo = new ComboBox<>();
    private final TextArea inputArea = new TextArea();
    private final Button loadCsvButton = new Button("Load CSV...");
    private final Button submitButton = new Button("Submit");
    private final Button clearButton = new Button("Clear");
    private final Label coordinatorLabel = new Label("Coordinator: none");

    // Dashboard cards.
    private final Label serverStatePill = pill("OFFLINE", UITheme.PillState.IDLE);
    private final Label serverStateDetail = detail("not connected");
    private final Label coordinatorPill = pill("UNKNOWN", UITheme.PillState.IDLE);
    private final Label coordinatorValue = bigValue("None");
    private final Label workersValue = bigValue("0/0");
    private final Label workersDetail = detail("no membership snapshot yet");
    private final Label jobsValue = bigValue("0");
    private final Label jobsDetail = detail("0 done · 0 failed");
    private final Label lastEventValue = detail("—");
    private final Label lastEventDetail = detail("no events yet");

    // Event timeline.
    private final ObservableList<TimelineEntry> timeline =
            FXCollections.observableArrayList();
    private final ListView<TimelineEntry> timelineList = new ListView<>(timeline);

    // Task table + live progress.
    private final ObservableList<TaskRow> taskRows = FXCollections.observableArrayList();
    private final TableView<TaskRow> taskTable = new TableView<>(taskRows);
    private final ObservableList<WorkerRow> workerRows = FXCollections.observableArrayList();
    private final TableView<WorkerRow> workerTable = new TableView<>(workerRows);
    private final ProgressBar progressBar = new ProgressBar(0);
    private final Label progressValue = new Label("—");
    private final TextArea logArea = new TextArea();

    // ---------------------------------------------------------------- state
    private final ExecutorService jobExecutor = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "client-job-executor");
        t.setDaemon(true);
        return t;
    });
    private final AtomicInteger jobSequence = new AtomicInteger(0);
    private final AtomicInteger jobsDone = new AtomicInteger(0);
    private final AtomicInteger jobsFailed = new AtomicInteger(0);
    private final Map<Integer, TaskState> taskStates = new ConcurrentHashMap<>();
    private final String clientId = "client-" + UUID.randomUUID().toString().substring(0, 8);
    /** Display name of this GUI instance - several clients run at once and share one cluster. */
    private volatile String instanceName = "Client";

    private ClusterMonitor monitor;
    private volatile String bootstrapHost = "localhost";
    private volatile int bootstrapPort = EndpointDefaults.BOOTSTRAP_PORT;
    private volatile BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap;
    private volatile int coordinatorId = EndpointDefaults.NO_COORDINATOR;
    private volatile String coordinatorHost = null;
    private volatile int coordinatorPort = -1;
    private volatile int clusterSize = ClusterConfig.WORKER_COUNT;

    public static void main(String[] args) {
        Application.launch(ClientApp.class, args);
    }

    /**
     * The JavaFX {@link Application} itself.
     *
     * <p>Kept separate from {@link ClientGUI} on purpose: modern JDK launchers
     * refuse to start a <i>main</i> class that extends
     * {@code javafx.application.Application} when JavaFX sits only on the
     * classpath ("JavaFX runtime components are missing"). A plain main class
     * with a nested application class runs fine with {@code java -cp ...}.</p>
     */
    public static final class ClientApp extends Application {
        @Override
        public void start(Stage stage) {
            new ClientGUI().start(stage, getParameters().getRaw());
        }
    }

    /**
     * Builds and shows the UI. {@code rawParams} carries the optional
     * {@code [bootstrapPort] [instanceName]} arguments, so several client windows
     * can be started side by side and told apart at a glance.
     */
    void start(Stage stage, List<String> rawParams) {
        if (!rawParams.isEmpty()) {
            try {
                bootstrapPortField.setText(
                        String.valueOf(Integer.parseInt(rawParams.get(0).trim())));
            } catch (NumberFormatException ignored) {
                // not a port number - keep the default
            }
        }
        if (rawParams.size() > 1 && !rawParams.get(1).isBlank()) {
            instanceName = rawParams.get(1).trim();
        }
        UITheme.install();
        monitor = new ClusterMonitor(this);

        BorderPane root = new BorderPane();
        root.setPadding(new Insets(16));
        root.setTop(buildHeader());
        root.setCenter(buildBody());

        Scene scene = new Scene(root, 1360, 940);
        UITheme.apply(scene);
        stage.setTitle(instanceName + " - Distributed Cluster Client - " + clientId);
        stage.setScene(scene);
        stage.setMinWidth(1080);
        stage.setMinHeight(760);
        stage.setOnCloseRequest(e -> shutdown());
        stage.show();

        updateHint();
        appendLog("Client ready (" + clientId + "). Connect to the Bootstrap Node, then submit jobs.");
        appendTimeline(ClusterMonitor.Severity.INFO,
                "Client started - press Connect to begin health monitoring");
    }

    private void shutdown() {
        monitor.shutdown();
        jobExecutor.shutdownNow();
        Platform.exit();
    }

    // ---------------------------------------------------------------- header

    private Node buildHeader() {
        Label title = new Label("Distributed Cluster Client");
        title.getStyleClass().add("title-text");
        Label subtitle = new Label(
                "MAX · PRIMECOUNT · PRIMESUM   ·   live progress   ·   automatic failover & resume");
        subtitle.getStyleClass().add("subtitle-text");

        HBox connection = buildConnectionBar();
        VBox header = new VBox(6, title, subtitle, connection);
        header.setPadding(new Insets(0, 0, 4, 0));
        return header;
    }

    private HBox buildConnectionBar() {
        Label hostLabel = muted("Bootstrap host");
        Label portLabel = muted("Port");
        hostField.setPrefWidth(150);
        bootstrapPortField.setPrefWidth(90);
        connectButton.getStyleClass().addAll(Styles.ACCENT, Styles.ROUNDED);
        connectButton.setOnAction(e -> connect());
        connectionStatus.getStyleClass().add("muted");

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox bar = new HBox(10, hostLabel, hostField, portLabel, bootstrapPortField,
                connectButton, connectionStatus, spacer);
        bar.setAlignment(Pos.CENTER_LEFT);
        bar.setPadding(new Insets(6, 0, 0, 0));
        return bar;
    }

    // -------------------------------------------------------------- dashboard

    private Node buildDashboard() {
        GridPane grid = new GridPane();
        grid.setHgap(12);
        grid.setVgap(12);

        ColumnConstraints c1 = new ColumnConstraints();
        c1.setPercentWidth(25);
        ColumnConstraints c2 = new ColumnConstraints();
        c2.setPercentWidth(25);
        ColumnConstraints c3 = new ColumnConstraints();
        c3.setPercentWidth(25);
        ColumnConstraints c4 = new ColumnConstraints();
        c4.setPercentWidth(25);
        grid.getColumnConstraints().addAll(c1, c2, c3, c4);

        VBox serverCard = card("Cluster Server",
                serverStatePill, serverStateDetail);
        VBox coordinatorCard = card("Coordinator",
                coordinatorValue, coordinatorPill);
        VBox workersCard = card("Workers Online", workersValue, workersDetail);
        VBox jobsCard = card("Jobs Submitted", jobsValue, jobsDetail);

        grid.add(serverCard, 0, 0);
        grid.add(coordinatorCard, 1, 0);
        grid.add(workersCard, 2, 0);
        grid.add(jobsCard, 3, 0);

        // Full-width timeline card underneath the four status cards.
        configureTimeline();
        Label timelineTitle = new Label("Event Timeline  ·  server running · failure detection · reconnect · recovery");
        timelineTitle.getStyleClass().add("card-title");
        HBox lastEventRow = new HBox(8, muted("Last event"), lastEventValue, lastEventDetail);
        lastEventRow.setAlignment(Pos.CENTER_LEFT);
        VBox timelineCard = cardNode(timelineTitle, lastEventRow, timelineList);
        VBox.setVgrow(timelineList, Priority.ALWAYS);
        timelineCard.setMinHeight(150);
        grid.add(timelineCard, 0, 1, 4, 1);

        VBox wrapper = new VBox(grid);
        VBox.setVgrow(grid, Priority.ALWAYS);
        return wrapper;
    }

    private VBox card(String title, Node... rows) {
        Label heading = new Label(title);
        heading.getStyleClass().add("card-title");
        return cardNode(heading, rows);
    }

    private VBox cardNode(Node heading, Node... rows) {
        VBox box = new VBox(8, heading);
        box.getStyleClass().add("card");
        box.setPadding(new Insets(14, 16, 14, 16));
        box.getChildren().addAll(rows);
        return box;
    }

    private static Label pill(String text, UITheme.PillState state) {
        Label label = new Label(text);
        label.getStyleClass().add("pill");
        UITheme.setPillState(label, state);
        return label;
    }

    private static Label bigValue(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("card-value");
        return label;
    }

    private static Label detail(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("card-detail");
        label.setWrapText(true);
        return label;
    }

    private static Label muted(String text) {
        Label label = new Label(text);
        label.getStyleClass().add("muted");
        return label;
    }

    // ------------------------------------------------------------------ body

    private Node buildBody() {
        Node dashboard = buildDashboard();

        // Left column: job submission + tasks + live progress.
        VBox jobCard = buildJobCard();
        VBox taskCard = buildTaskCard();
        VBox progressCard = buildProgressCard();
        VBox left = new VBox(12, jobCard, taskCard, progressCard);
        VBox.setVgrow(taskCard, Priority.ALWAYS);

        // Right column: event log.
        VBox logCard = buildLogCard();

        SplitPane rightSplit = new SplitPane(left, logCard);
        rightSplit.setDividerPositions(0.74);

        SplitPane mainSplit = new SplitPane(dashboard, rightSplit);
        mainSplit.setDividerPositions(0.34);
        VBox.setVgrow(mainSplit, Priority.ALWAYS);

        VBox body = new VBox(12, mainSplit);
        VBox.setVgrow(mainSplit, Priority.ALWAYS);
        return body;
    }

    private VBox buildJobCard() {
        Label heading = new Label("Submit Job");
        heading.getStyleClass().add("card-title");

        jobTypeCombo.getItems().addAll(JOB_TYPES);
        jobTypeCombo.setValue("PRIMESUM");
        jobTypeCombo.valueProperty().addListener((o, a, b) -> updateHint());
        coordinatorLabel.getStyleClass().add("muted");

        inputArea.setPrefRowCount(3);
        inputArea.setWrapText(true);
        inputArea.getStyleClass().add("mono-input");

        loadCsvButton.getStyleClass().add(Styles.FLAT);
        loadCsvButton.setOnAction(e -> loadCsv());
        submitButton.getStyleClass().addAll(Styles.ACCENT, Styles.ROUNDED);
        submitButton.setOnAction(e -> submitJob());
        clearButton.getStyleClass().add(Styles.FLAT);
        clearButton.setOnAction(e -> {
            inputArea.clear();
            appendLog("Input cleared");
        });

        Region spacer = new Region();
        HBox.setHgrow(spacer, Priority.ALWAYS);
        HBox buttons = new HBox(10, loadCsvButton, submitButton, clearButton, spacer);
        buttons.setAlignment(Pos.CENTER_LEFT);

        Region gap = new Region();
        HBox.setHgrow(gap, Priority.ALWAYS);
        HBox typeRow = new HBox(10, muted("Job type"), jobTypeCombo, gap, coordinatorLabel);
        typeRow.setAlignment(Pos.CENTER_LEFT);

        VBox box = new VBox(10, heading, typeRow, inputArea, buttons);
        box.getStyleClass().add("card");
        box.setPadding(new Insets(14, 16, 14, 16));
        return box;
    }

    private VBox buildTaskCard() {
        Label heading = new Label("Tasks  ·  concurrent submissions supported");
        heading.getStyleClass().add("card-title");

        taskTable.setPlaceholder(new Label("No jobs submitted yet"));
        taskTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        taskTable.setPrefHeight(260);
        taskTable.getSelectionModel().setSelectionMode(SelectionMode.SINGLE);
        taskTable.getSelectionModel().selectedItemProperty().addListener((o, a, b) -> {
            if (b != null) {
                showSelectedProgress(b);
            }
        });

        TableColumn<TaskRow, String> idCol = textColumn("Job", r -> r.id, 60);
        TableColumn<TaskRow, String> typeCol = textColumn("Type", r -> r.type, 110);
        TableColumn<TaskRow, String> submittedCol = textColumn("Submitted", r -> r.submitted, 90);
        TableColumn<TaskRow, String> statusCol = textColumn("Status", r -> r.status, 130);
        TableColumn<TaskRow, String> coordinatorCol = textColumn("Coordinator", r -> r.coordinator, 100);
        TableColumn<TaskRow, String> workersCol = textColumn("Workers", r -> r.workers, 110);
        TableColumn<TaskRow, String> resultCol = textColumn("Result", r -> r.result, 130);
        TableColumn<TaskRow, String> errorCol = textColumn("Error", r -> r.error, 160);

        // Colour the Status cell: green when done, red when failed, amber while running.
        statusCol.setCellFactory(col -> new TableCell<>() {
            @Override
            protected void updateItem(String item, boolean empty) {
                super.updateItem(item, empty);
                getStyleClass().removeAll("text-ok", "text-warn", "text-bad");
                if (empty || item == null) {
                    setText(null);
                    return;
                }
                setText(item);
                if (item.startsWith("Done")) {
                    UITheme.addStyle(this, "text-ok");
                } else if (item.startsWith("Failed")) {
                    UITheme.addStyle(this, "text-bad");
                } else if (item.startsWith("Running")) {
                    UITheme.addStyle(this, "text-warn");
                }
            }
        });

        taskTable.getColumns().addAll(idCol, typeCol, submittedCol, statusCol,
                coordinatorCol, workersCol, resultCol, errorCol);

        VBox box = new VBox(10, heading, taskTable);
        box.getStyleClass().add("card");
        box.setPadding(new Insets(14, 16, 14, 16));
        VBox.setVgrow(taskTable, Priority.ALWAYS);
        return box;
    }

    private VBox buildProgressCard() {
        Label heading = new Label("Live Progress  ·  select a task row for its worker drill-down");
        heading.getStyleClass().add("card-title");

        progressBar.setPrefWidth(Double.MAX_VALUE);
        progressBar.setMinHeight(18);
        HBox.setHgrow(progressBar, Priority.ALWAYS);
        progressValue.getStyleClass().add("mono");
        progressValue.setMinWidth(64);
        progressValue.setAlignment(Pos.CENTER_RIGHT);

        HBox barRow = new HBox(10, progressBar, progressValue);
        barRow.setAlignment(Pos.CENTER_LEFT);

        workerTable.setPlaceholder(new Label("Select a task to inspect its workers"));
        workerTable.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        workerTable.setPrefHeight(150);

        TableColumn<WorkerRow, String> wId = textColumn("Worker", r -> r.workerId, 80);
        TableColumn<WorkerRow, String> wAssigned = textColumn("Assigned", r -> r.assigned, 90);
        TableColumn<WorkerRow, String> wCompleted = textColumn("Completed", r -> r.completed, 95);
        TableColumn<WorkerRow, String> wJac = textColumn("JAC", r -> r.jac, 70);
        TableColumn<WorkerRow, String> wOnline = textColumn("Online", r -> r.online, 85);
        TableColumn<WorkerRow, String> wStatus = textColumn("Status", r -> r.status, 150);
        workerTable.getColumns().addAll(wId, wAssigned, wCompleted, wJac, wOnline, wStatus);

        VBox box = new VBox(10, heading, barRow, workerTable);
        box.getStyleClass().add("card");
        box.setPadding(new Insets(14, 16, 14, 16));
        return box;
    }

    private VBox buildLogCard() {
        Label heading = new Label("Log");
        heading.getStyleClass().add("card-title");

        logArea.setEditable(false);
        logArea.setWrapText(true);
        logArea.setPrefRowCount(10);
        logArea.getStyleClass().add("log-area");

        VBox box = new VBox(10, heading, logArea);
        box.getStyleClass().add("card");
        box.setPadding(new Insets(14, 16, 14, 16));
        VBox.setVgrow(logArea, Priority.ALWAYS);
        return box;
    }

    // ------------------------------------------------------- cluster monitor

    /** Configures the event timeline list (severity-coloured custom cells). */
    private void configureTimeline() {
        timelineList.getStyleClass().add("event-list");
        timelineList.setCellFactory(lv -> new ListCell<TimelineEntry>() {
            private final Label time = new Label();
            private final Label msg = new Label();
            private final HBox box = new HBox(10, time, msg);

            {
                time.getStyleClass().add("event-time");
                msg.getStyleClass().add("event-msg");
                msg.setWrapText(true);
                HBox.setHgrow(msg, Priority.ALWAYS);
                box.getStyleClass().add("event-cell");
                box.setAlignment(Pos.CENTER_LEFT);
            }

            @Override
            protected void updateItem(TimelineEntry item, boolean empty) {
                super.updateItem(item, empty);
                if (empty || item == null) {
                    setText(null);
                    setGraphic(null);
                    return;
                }
                time.setText(item.time());
                msg.setText(item.message());
                msg.getStyleClass().removeAll("event-msg-info", "event-msg-success",
                        "event-msg-warning", "event-msg-danger");
                msg.getStyleClass().add(switch (item.severity()) {
                    case SUCCESS -> "event-msg-success";
                    case WARNING -> "event-msg-warning";
                    case ERROR -> "event-msg-danger";
                    default -> "event-msg-info";
                });
                setGraphic(box);
            }
        });
    }

    /**
     * Server lifecycle (RUNNING / FAILURE DETECTED / RECONNECTING / RECOVERED /
     * OFFLINE) reported by {@link ClusterMonitor} on its probe thread.
     */
    @Override
    public void onServerState(ClusterMonitor.ServerState state, String detail,
                              int consecutiveFailures, long outageMs) {
        Platform.runLater(() -> {
            switch (state) {
                case OFFLINE -> setServerPill("OFFLINE", UITheme.PillState.IDLE, "not connected");
                case RUNNING -> setServerPill("RUNNING", UITheme.PillState.SUCCESS, detail);
                case FAILURE_DETECTED -> setServerPill("FAILURE DETECTED", UITheme.PillState.DANGER, detail);
                case RECONNECTING -> setServerPill("RECONNECTING", UITheme.PillState.WARNING,
                        "attempt " + consecutiveFailures + " · down for "
                                + ClusterMonitor.formatDuration(outageMs));
                case RECOVERED -> setServerPill("RECOVERED", UITheme.PillState.SUCCESS, detail);
            }
        });
    }

    private void setServerPill(String text, UITheme.PillState state, String detailText) {
        serverStatePill.setText(text);
        UITheme.setPillState(serverStatePill, state);
        serverStateDetail.setText(detailText);
    }

    /** Coordinator lifecycle (failover / recovery) reported by the monitor. */
    @Override
    public void onCoordinatorState(ClusterMonitor.CoordinatorState state, String detail) {
        Platform.runLater(() -> {
            switch (state) {
                case UNKNOWN -> {
                    coordinatorPill.setText("UNKNOWN");
                    UITheme.setPillState(coordinatorPill, UITheme.PillState.IDLE);
                }
                case ONLINE -> {
                    coordinatorPill.setText("ONLINE");
                    UITheme.setPillState(coordinatorPill, UITheme.PillState.SUCCESS);
                }
                case FAILURE_DETECTED -> {
                    coordinatorPill.setText("FAILURE DETECTED");
                    UITheme.setPillState(coordinatorPill, UITheme.PillState.DANGER);
                }
                case RECOVERED -> {
                    coordinatorPill.setText("RECOVERED");
                    UITheme.setPillState(coordinatorPill, UITheme.PillState.SUCCESS);
                }
            }
        });
    }

    /** Fresh worker membership snapshot (online/expected counts + registered list). */
    @Override
    public void onWorkersSnapshot(int online, int total, List<WorkerInfo> workers) {
        Platform.runLater(() -> {
            workersValue.setText(online + "/" + total);
            workersDetail.setText(workers.size() + " registered · updated "
                    + LocalTime.now().format(CLOCK));
        });
    }

    /** Timeline event emitted by the monitor (state changes, failures, recovery). */
    @Override
    public void onEvent(ClusterMonitor.Severity severity, String message) {
        Platform.runLater(() -> appendTimeline(severity, message));
    }

    // ------------------------------------------------------------- connection

    private void connect() {
        String host = hostField.getText().trim();
        bootstrapHost = host.isEmpty() ? "localhost" : host;
        try {
            bootstrapPort = Integer.parseInt(bootstrapPortField.getText().trim());
        } catch (NumberFormatException e) {
            showError("Bootstrap port must be a number");
            return;
        }

        connectButton.setDisable(true);
        connectionStatus.setText("Connecting...");
        jobExecutor.submit(this::doConnect);
    }

    private void doConnect() {
        try {
            BootstrapServiceGrpc.BootstrapServiceBlockingStub foundBootstrap =
                    GrpcChannels.bootstrap(bootstrapHost, bootstrapPort);
            bootstrap = foundBootstrap;
            List<WorkerInfo> active =
                    foundBootstrap.getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
            final int registered = active.size();
            try {
                clusterSize = foundBootstrap.getClusterSize(Empty.getDefaultInstance()).getSize();
            } catch (Exception e) {
                clusterSize = Math.max(registered, ClusterConfig.WORKER_COUNT);
            }
            final int elected = refreshCoordinator() ? coordinatorId : EndpointDefaults.NO_COORDINATOR;

            // Health dashboard starts probing the server from here on.
            monitor.start(bootstrapHost, bootstrapPort);

            Platform.runLater(() -> {
                connectButton.setDisable(false);
                workersValue.setText(registered + "/" + clusterSize);
                if (elected == EndpointDefaults.NO_COORDINATOR) {
                    connectionStatus.setText("Connected - no coordinator yet");
                    setCoordinatorUI(EndpointDefaults.NO_COORDINATOR);
                } else {
                    connectionStatus.setText("Online - coordinator worker " + elected);
                    setCoordinatorUI(elected);
                }
                appendLog("Connected to Bootstrap Node at " + bootstrapHost + ":" + bootstrapPort
                        + " - " + registered + " worker(s) registered"
                        + (elected == EndpointDefaults.NO_COORDINATOR
                                ? ", no coordinator elected yet"
                                : ", coordinator is worker " + elected));
            });
        } catch (Exception e) {
            Platform.runLater(() -> {
                connectButton.setDisable(false);
                connectionStatus.setText("Connection failed");
                showError("Could not connect: " + ClusterMonitor.rootMessage(e));
            });
        }
    }

    /**
     * Asks every registered worker which coordinator it knows and returns the
     * agreed coordinator id (or {@link EndpointDefaults#NO_COORDINATOR} if none).
     *
     * @param avoidWorkerId skip this worker (the one that just rejected a
     *                      submission, whose term is over) so a retry never
     *                      resolves the same just-ended coordinator again.
     */
    private int findCoordinatorId(BootstrapServiceGrpc.BootstrapServiceBlockingStub currentBootstrap,
                                  int avoidWorkerId) {
        List<WorkerInfo> workers =
                currentBootstrap.getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
        int reported = EndpointDefaults.NO_COORDINATOR;
        for (WorkerInfo info : workers) {
            try {
                int candidate = GrpcChannels.cluster(info.getHost(), info.getPort())
                        .withDeadlineAfter(1500, TimeUnit.MILLISECONDS)
                        .getCurrentCoordinatorId(Empty.getDefaultInstance()).getValue();
                if (candidate != EndpointDefaults.NO_COORDINATOR && candidate != avoidWorkerId) {
                    reported = candidate;
                    break;
                }
            } catch (Exception ignored) {
                // worker not answering - try the next one
            }
        }
        if (reported == EndpointDefaults.NO_COORDINATOR) {
            return EndpointDefaults.NO_COORDINATOR;
        }
        // Confirm the reported coordinator is alive and self-consistent; a stale
        // report can otherwise point at a worker that died mid-failover.
        for (WorkerInfo info : workers) {
            if (info.getWorkerId() != reported) {
                continue;
            }
            try {
                int self = GrpcChannels.cluster(info.getHost(), info.getPort())
                        .withDeadlineAfter(1500, TimeUnit.MILLISECONDS)
                        .getCurrentCoordinatorId(Empty.getDefaultInstance()).getValue();
                return self == reported ? reported : EndpointDefaults.NO_COORDINATOR;
            } catch (Exception e) {
                return EndpointDefaults.NO_COORDINATOR;
            }
        }
        return EndpointDefaults.NO_COORDINATOR;
    }

    private static WorkerInfo findWorker(List<WorkerInfo> workers, int workerId) {
        for (WorkerInfo info : workers) {
            if (info.getWorkerId() == workerId) {
                return info;
            }
        }
        return null;
    }

    /**
     * Re-queries the Bootstrap Node for the currently elected coordinator and
     * points the client's submit path at it. Used on connect and when a stream
     * breaks because a term ended or the coordinator died.
     *
     * @param avoidWorkerId a worker the client must not resolve again (the one
     *                      that just rejected the job), or
     *                      {@link EndpointDefaults#NO_COORDINATOR}. Without it a
     *                      retry could pick the coordinator whose term has only
     *                      just ended and be rejected a second time.
     */
    private boolean refreshCoordinator(int avoidWorkerId) {
        final BootstrapServiceGrpc.BootstrapServiceBlockingStub currentBootstrap = bootstrap;
        if (currentBootstrap == null) {
            return false;
        }
        try {
            List<WorkerInfo> active =
                    currentBootstrap.getActiveWorkers(Empty.getDefaultInstance()).getWorkersList();
            int coordId = findCoordinatorId(currentBootstrap, avoidWorkerId);
            if (coordId == EndpointDefaults.NO_COORDINATOR) {
                return false;
            }
            WorkerInfo coordInfo = findWorker(active, coordId);
            if (coordInfo == null) {
                return false;
            }
            coordinatorId = coordId;
            coordinatorHost = coordInfo.getHost();
            coordinatorPort = coordInfo.getPort();
            setCoordinatorUI(coordId);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean refreshCoordinator() {
        return refreshCoordinator(EndpointDefaults.NO_COORDINATOR);
    }

    /** Updates the coordinator widgets and the monitor's probe target (FX-thread safe). */
    private void setCoordinatorUI(final int elected) {
        Platform.runLater(() -> {
            if (elected == EndpointDefaults.NO_COORDINATOR) {
                coordinatorLabel.setText("Coordinator: none (workers are electing...)");
                coordinatorValue.setText("None");
                coordinatorPill.setText("ELECTING");
                UITheme.setPillState(coordinatorPill, UITheme.PillState.WARNING);
                monitor.clearCoordinator();
            } else {
                coordinatorLabel.setText("Coordinator: worker " + elected);
                coordinatorValue.setText("Worker " + elected);
                coordinatorPill.setText("ONLINE");
                UITheme.setPillState(coordinatorPill, UITheme.PillState.SUCCESS);
                monitor.setCoordinator(elected, coordinatorHost, coordinatorPort);
            }
        });
    }

    // ------------------------------------------------------------------ jobs

    private void submitJob() {
        if (coordinatorHost == null || coordinatorPort < 0) {
            showError("No coordinator - press Connect (workers elect one automatically)");
            return;
        }

        String type = jobTypeCombo.getValue();
        String raw = inputArea.getText().trim();
        if (raw.isEmpty()) {
            showError("Input is empty");
            return;
        }

        JobRequest request;
        try {
            request = buildRequest(type, raw);
        } catch (Exception e) {
            showError(ClusterMonitor.rootMessage(e));
            return;
        }

        final int jobId = jobSequence.incrementAndGet();
        final String submitted = LocalTime.now().format(CLOCK);
        TaskState state = new TaskState();
        taskStates.put(jobId, state);

        TaskRow row = new TaskRow(jobId, type, submitted);
        row.status.set("Running 0%");
        row.coordinator.set(String.valueOf(coordinatorId));
        addTaskRow(row);
        jobsValue.setText(String.valueOf(jobSequence.get()));
        selectTaskRow(jobId);
        appendLog("Job " + jobId + " [" + type + "] submitted to coordinator worker " + coordinatorId);

        streamJob(jobId, request, state);
    }

    private JobRequest buildRequest(String type, String raw) {
        JobRequest.Builder builder = JobRequest.newBuilder().setClientId(clientId);
        switch (type) {
            case "MAX" -> builder.setJobType(JobType.MAX).addAllNumbers(parseNumbers(raw));
            case "PRIMECOUNT" -> builder.setJobType(JobType.PRIMECOUNT).addAllNumbers(parseNumbers(raw));
            case "PRIMESUM" -> {
                int[] range = parseRange(raw);
                builder.setJobType(JobType.PRIMESUM).setRangeStart(range[0]).setRangeEnd(range[1]);
            }
            default -> throw new IllegalArgumentException("Unknown job type: " + type);
        }
        return builder.build();
    }

    /** Opens the async gRPC streaming channel to the current coordinator. */
    private void streamJob(int jobId, JobRequest request, TaskState state) {
        final String host = coordinatorHost;
        final int port = coordinatorPort;
        final ClientServiceGrpc.ClientServiceStub stub = GrpcChannels.clientAsync(host, port)
                .withDeadlineAfter(STREAM_DEADLINE_SECONDS, TimeUnit.SECONDS);
        try {
            stub.submitJob(request, new io.grpc.stub.StreamObserver<TaskUpdate>() {
                @Override
                public void onNext(TaskUpdate update) {
                    Platform.runLater(() -> handleProgress(jobId, update, state));
                }

                @Override
                public void onError(Throwable t) {
                    Platform.runLater(() -> handleStreamError(jobId, request, state, t));
                }

                @Override
                public void onCompleted() {
                    // final onNext (TASK_DONE) already rendered the outcome
                }
            });
        } catch (Exception e) {
            Platform.runLater(() -> handleStreamError(jobId, request, state, e));
        }
    }

    private void handleProgress(int jobId, TaskUpdate update, TaskState state) {
        state.taskId = update.getTaskId();
        state.percent = update.getPercent();
        state.workers = workerRows(update);
        state.usedWorkers = update.getUsedWorkerIdsList();
        state.coordinatorId = update.getCoordinatorId();

        TaskStatus status = update.getStatus();
        if (status == TaskStatus.TASK_DONE) {
            String result = "result=" + update.getResult();
            jobsDone.incrementAndGet();
            updateJobsDetail();
            updateTaskRow(jobId, "Done", String.valueOf(update.getCoordinatorId()),
                    joinIds(state.usedWorkers), result, null);
            appendLog("Job " + jobId + " [" + update.getJobType() + "] finished -> " + result);
        } else if (status == TaskStatus.TASK_FAILED) {
            jobsFailed.incrementAndGet();
            updateJobsDetail();
            updateTaskRow(jobId, "Failed", String.valueOf(update.getCoordinatorId()),
                    "-", "-", update.getMessage());
            appendLog("Job " + jobId + " failed -> " + update.getMessage());
        } else {
            updateTaskRow(jobId, "Running " + update.getPercent() + "%",
                    String.valueOf(update.getCoordinatorId()),
                    joinIds(state.usedWorkers), "-", null);
        }
        if (isSelectedTask(jobId)) {
            renderProgress(jobId, state);
        }
    }

    /** Stream broke: coordinator term ended, coordinator died, or another transport failure. */
    private void handleStreamError(int jobId, JobRequest request, TaskState state, Throwable t) {
        String message = ClusterMonitor.rootMessage(t);
        if (shouldResumeAfter(t, message)) {
            state.attempt++;
            if (state.attempt > MAX_RESUME_ATTEMPTS) {
                jobsFailed.incrementAndGet();
                updateJobsDetail();
                updateTaskRow(jobId, "Failed", "-", "-", "-", message + " (coordinator re-elected)");
                appendLog("Job " + jobId + " failed after " + MAX_RESUME_ATTEMPTS
                        + " resume attempt(s) -> " + message);
                return;
            }
            appendLog("Stream interrupted (attempt " + state.attempt + "): " + message
                    + " - re-resolving the coordinator and resuming job " + jobId);
            final TaskState captured = state;
            // The coordinator that rejected this job has ended its term; resolving
            // it again would be rejected a second time, so skip it while retrying.
            final int rejectedBy = message != null && message.contains("Coordinator term ended")
                    ? captured.coordinatorId
                    : EndpointDefaults.NO_COORDINATOR;
            jobExecutor.submit(() -> {
                boolean resolved = false;
                for (int i = 0; i < MAX_RESUME_ATTEMPTS && !resolved; i++) {
                    resolved = refreshCoordinator(rejectedBy) && coordinatorId != EndpointDefaults.NO_COORDINATOR;
                    if (!resolved) {
                        try {
                            Thread.sleep(RESOLVE_WAIT_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                final boolean ok = resolved;
                final String resumeTaskId = captured.taskId == null ? "" : captured.taskId;
                Platform.runLater(() -> {
                    if (ok) {
                        JobRequest resume = request.toBuilder().setResumeTaskId(resumeTaskId).build();
                        appendLog("Resuming job " + jobId + " from task " + shortTaskId(resumeTaskId)
                                + " via coordinator worker " + coordinatorId);
                        streamJob(jobId, resume, captured);
                    } else {
                        jobsFailed.incrementAndGet();
                        updateJobsDetail();
                        updateTaskRow(jobId, "Failed", "-", "-", "-", message);
                        appendLog("Job " + jobId
                                + " failed - no coordinator could be resolved -> " + message);
                    }
                });
            });
        } else {
            jobsFailed.incrementAndGet();
            updateJobsDetail();
            updateTaskRow(jobId, "Failed", "-", "-", "-", message);
            appendLog("Job " + jobId + " failed -> " + message);
        }
    }

    private boolean shouldResumeAfter(Throwable t, String message) {
        if (message != null && (message.contains("not the coordinator")
                || message.contains("Coordinator term ended"))) {
            return true;
        }
        if (t instanceof StatusRuntimeException sre) {
            return sre.getStatus().getCode() == io.grpc.Status.Code.UNAVAILABLE
                    || sre.getStatus().getCode() == io.grpc.Status.Code.DEADLINE_EXCEEDED
                    || sre.getStatus().getCode() == io.grpc.Status.Code.CANCELLED;
        }
        return false;
    }

    private List<WorkerRow> workerRows(TaskUpdate update) {
        List<WorkerRow> rows = new ArrayList<>();
        for (WorkerProgress wp : update.getWorkersList()) {
            rows.add(new WorkerRow(wp.getWorkerId(), wp.getTasksAssigned(), wp.getTasksCompleted(),
                    wp.getJac(), wp.getOnline(), wp.getStatus()));
        }
        return rows;
    }

    // -------------------------------------------------------- progress view

    private void showSelectedProgress(TaskRow row) {
        TaskState state = taskStates.get(row.jobId);
        if (state == null) {
            progressBar.setProgress(0);
            progressValue.setText("—");
            workerRows.setAll();
            return;
        }
        renderProgress(row.jobId, state);
    }

    private void renderProgress(int jobId, TaskState state) {
        progressBar.setProgress(state.percent / 100.0);
        progressValue.setText(state.percent + "%");
        workerRows.setAll(state.workers);
    }

    private boolean isSelectedTask(int jobId) {
        TaskRow selected = taskTable.getSelectionModel().getSelectedItem();
        return selected != null && selected.jobId == jobId;
    }

    private void selectTaskRow(int jobId) {
        for (int i = 0; i < taskRows.size(); i++) {
            if (taskRows.get(i).jobId == jobId) {
                taskTable.getSelectionModel().select(i);
                taskTable.scrollTo(i);
                return;
            }
        }
    }

    // ---------------------------------------------------------- input parsing

    private void loadCsv() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Load CSV input");
        chooser.getExtensionFilters().addAll(
                new FileChooser.ExtensionFilter("CSV / text files", "*.csv", "*.txt"),
                new FileChooser.ExtensionFilter("All files", "*.*"));
        Stage owner = inputArea.getScene() == null
                ? null : (Stage) inputArea.getScene().getWindow();
        File file = chooser.showOpenDialog(owner);
        if (file == null) {
            return;
        }
        try {
            String content = Files.readString(file.toPath()).trim();
            inputArea.setText(content);
            appendLog("Loaded CSV from " + file.getName());
        } catch (IOException e) {
            showError("Could not read CSV file: " + e.getMessage());
        }
    }

    private void updateHint() {
        String type = jobTypeCombo.getValue();
        if (type == null) {
            return;
        }
        String hint = "PRIMESUM".equals(type)
                ? "start,end  e.g. 1,1000"
                : "comma-separated integers  e.g. 12,5,99,2,17";
        inputArea.setPromptText(hint);
        inputArea.setTooltip(new Tooltip(hint));
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

    // ---------------------------------------------------------------- helpers

    private void addTaskRow(TaskRow row) {
        if (Platform.isFxApplicationThread()) {
            taskRows.add(row);
        } else {
            Platform.runLater(() -> taskRows.add(row));
        }
    }

    private void updateTaskRow(int jobId, String status, String coordinator,
                               String workers, String result, String error) {
        Runnable update = () -> {
            for (TaskRow row : taskRows) {
                if (row.jobId == jobId) {
                    row.status.set(status);
                    row.coordinator.set(coordinator);
                    row.workers.set(workers == null ? "-" : workers);
                    row.result.set(result == null ? "-" : result);
                    row.error.set(error == null ? "" : error);
                    return;
                }
            }
        };
        if (Platform.isFxApplicationThread()) {
            update.run();
        } else {
            Platform.runLater(update);
        }
    }

    private void updateJobsDetail() {
        String text = jobsDone.get() + " done · " + jobsFailed.get() + " failed";
        if (Platform.isFxApplicationThread()) {
            jobsDetail.setText(text);
        } else {
            Platform.runLater(() -> jobsDetail.setText(text));
        }
    }

    private void appendLog(String message) {
        if (!Platform.isFxApplicationThread()) {
            Platform.runLater(() -> appendLog(message));
            return;
        }
        logArea.appendText("[" + LocalTime.now().format(CLOCK) + "] " + message + "\n");
        logArea.positionCaret(logArea.getLength());
    }

    /** Adds a timestamped, severity-coloured entry to the dashboard timeline + log. */
    private void appendTimeline(ClusterMonitor.Severity severity, String message) {
        if (!Platform.isFxApplicationThread()) {
            Platform.runLater(() -> appendTimeline(severity, message));
            return;
        }
        String time = LocalTime.now().format(CLOCK);
        timeline.add(0, new TimelineEntry(time, severity, message));
        while (timeline.size() > 250) {
            timeline.remove(timeline.size() - 1);
        }
        lastEventValue.setText(time);
        lastEventDetail.setText(message);
        appendLog(severity + ": " + message);
    }

    private void showError(String message) {
        appendLog("ERROR: " + message);
        Alert alert = new Alert(Alert.AlertType.ERROR, message, ButtonType.OK);
        alert.setTitle("Error");
        alert.setHeaderText(null);
        if (inputArea.getScene() != null
                && inputArea.getScene().getWindow() instanceof Stage stage) {
            alert.initOwner(stage);
        }
        alert.showAndWait();
    }

    private static <T> TableColumn<T, String> textColumn(String title,
            Function<T, ObservableValue<String>> getter, double minWidth) {
        TableColumn<T, String> column = new TableColumn<>(title);
        column.setCellValueFactory(data -> getter.apply(data.getValue()));
        column.setMinWidth(minWidth);
        return column;
    }

    private static String joinIds(List<Integer> workerIds) {
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

    // ------------------------------------------------------------ inner types

    /** Mutable per-job runtime state fed by the streaming SubmitJob updates. */
    private static final class TaskState {
        String taskId;
        int percent;
        int coordinatorId = EndpointDefaults.NO_COORDINATOR;
        List<WorkerRow> workers = new ArrayList<>();
        List<Integer> usedWorkers = new ArrayList<>();
        int attempt;
    }

    /** One row of the task table (observable properties keep the table live). */
    private static final class TaskRow {
        final int jobId;
        final StringProperty id = new SimpleStringProperty();
        final StringProperty type = new SimpleStringProperty();
        final StringProperty submitted = new SimpleStringProperty();
        final StringProperty status = new SimpleStringProperty();
        final StringProperty coordinator = new SimpleStringProperty();
        final StringProperty workers = new SimpleStringProperty();
        final StringProperty result = new SimpleStringProperty();
        final StringProperty error = new SimpleStringProperty();

        TaskRow(int jobId, String type, String submitted) {
            this.jobId = jobId;
            this.id.set(String.valueOf(jobId));
            this.type.set(type);
            this.submitted.set(submitted);
            this.status.set("-");
            this.coordinator.set("-");
            this.workers.set("-");
            this.result.set("-");
            this.error.set("");
        }
    }

    /** One row of the worker-level progress drill-down. */
    private static final class WorkerRow {
        final StringProperty workerId = new SimpleStringProperty();
        final StringProperty assigned = new SimpleStringProperty();
        final StringProperty completed = new SimpleStringProperty();
        final StringProperty jac = new SimpleStringProperty();
        final StringProperty online = new SimpleStringProperty();
        final StringProperty status = new SimpleStringProperty();

        WorkerRow(int workerId, int assigned, int completed, int jac,
                  boolean online, String status) {
            this.workerId.set(String.valueOf(workerId));
            this.assigned.set(String.valueOf(assigned));
            this.completed.set(String.valueOf(completed));
            this.jac.set(String.valueOf(jac));
            this.online.set(online ? "ONLINE" : "DOWN");
            this.status.set(status == null ? "-" : status);
        }
    }

    /** One timestamped entry of the event timeline. */
    private record TimelineEntry(String time, ClusterMonitor.Severity severity, String message) {
        @Override
        public String toString() {
            return time + "  " + severity + "  " + message;
        }
    }
}
