package com.personal.kafka.pilot.ui;

// --- Project Imports ---

import com.personal.kafka.pilot.engine.KafkaConsumerVerifier;
import com.personal.kafka.pilot.engine.KafkaLoadTestEngine;
import com.personal.kafka.pilot.engine.MessageGenerator;
import com.personal.kafka.pilot.model.FieldConfig;
import com.personal.kafka.pilot.model.KafkaBrokerConfig;
import com.personal.kafka.pilot.model.PodState;
import com.personal.kafka.pilot.model.TestConfiguration;
import com.personal.kafka.pilot.model.TestMetrics;
import com.personal.kafka.pilot.service.KafkaSearchService;
import com.personal.kafka.pilot.service.KafkaTailService;
import com.personal.kafka.pilot.util.ConfigurationManager;
import com.personal.kafka.pilot.util.ErrorHandler;
import com.personal.kafka.pilot.util.ExportService;
import com.personal.kafka.pilot.util.MavenDependencyResolver;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.chart.CategoryAxis;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.scene.control.*;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Desktop;
import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.ThreadMXBean;
import java.net.URI;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

// --- Kafka Imports ---
// --- JavaFX Imports ---
// --- Java Standard Library Imports ---
// --- Logging Imports ---

/**
 * Main controller for the Kafka Pilot UI.
 * Delegates Kafka consumer operations to {@link KafkaSearchService}.
 */
public class MainController {

    /**
     * Data class for partition offset information
     */
    public static class PartitionOffsetData {
        private final Integer partition;
        private final Long earliestOffset;
        private final Long latestOffset;
        private final Long messageCount;

        public PartitionOffsetData(Integer partition, Long earliestOffset, Long latestOffset) {
            this.partition = partition;
            this.earliestOffset = earliestOffset;
            this.latestOffset = latestOffset;
            this.messageCount = latestOffset - earliestOffset;
        }

        public Integer getPartition() { return partition; }
        public Long getEarliestOffset() { return earliestOffset; }
        public Long getLatestOffset() { return latestOffset; }
        public Long getMessageCount() { return messageCount; }
    }

    private static final Logger logger = LoggerFactory.getLogger(MainController.class);
    private static final String DEFAULT_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    // --- Centralized Error Handling ---
    private void handleError(String context, Exception e) {
        ErrorHandler.handleError(context, e, this::appendToConsole);
    }

    private void handleError(String prefix, String context, Exception e) {
        ErrorHandler.handleError(prefix, context, e, this::appendToConsole);
    }

    // --- FXML Fields: Common ---
    @FXML private TextField activePodsField;
    @FXML private ComboBox<KafkaBrokerConfig> selectedForWorkComboBox;  // Source of truth for all operations
    @FXML private TextField brokerNameField;
    @FXML private TextField brokerServersField;
    @FXML private Button addBrokerButton;
    @FXML private Button updateBrokerButton;
    @FXML private Button deleteBrokerButton;
    @FXML private Button connectButton;
    @FXML private TextField mavenDependencyField;
    @FXML private TextField mavenRepoUrlField;
    @FXML private TextArea consoleOutput;
    @FXML private Button toggleConsoleButton;
    @FXML private VBox pushConsoleBox;
    @FXML private TabPane mainTabPane;
    @FXML private Button clearConsoleButton;
    @FXML private Button exportConsoleButton;

    // --- FXML Fields: SSL/TLS Configuration ---
    @FXML private TitledPane sslConfigPane;
    @FXML private CheckBox sslEnabledCheckBox;
    @FXML private ComboBox<String> sslSecurityProtocolCombo;
    @FXML private TextField sslTruststoreLocationField;
    @FXML private PasswordField sslTruststorePasswordField;
    @FXML private ComboBox<String> sslTruststoreTypeCombo;
    @FXML private TextField sslKeystoreLocationField;
    @FXML private PasswordField sslKeystorePasswordField;
    @FXML private PasswordField sslKeyPasswordField;
    @FXML private ComboBox<String> sslKeystoreTypeCombo;
    @FXML private ComboBox<String> sslEndpointAlgorithmCombo;
    @FXML private Label sslConfigStatusLabel;

    // --- FXML Fields: Search Tab ---
    @FXML private ComboBox<String> searchTopicComboBox;
    @FXML private TextField searchTextFilter;
    @FXML private Button copySearchResultsButton;
    @FXML private RadioButton searchAllPartitions;
    @FXML private RadioButton searchSpecificPartition;
    @FXML private ComboBox<String> searchPartitionField;
    @FXML private TextField sourceOffsetField;
    @FXML private Label offsetLabel;
    @FXML private RadioButton searchNoTimeRange;
    @FXML private RadioButton searchWithTimeRange;
    @FXML private HBox searchFromTimeBox;
    @FXML private HBox searchToTimeBox;
    @FXML private DatePicker searchFromDatePicker;
    @FXML private DatePicker searchToDatePicker;
    @FXML private Spinner<Integer> searchFromHourSpinner;
    @FXML private Spinner<Integer> searchFromMinuteSpinner;
    @FXML private Spinner<Integer> searchToHourSpinner;
    @FXML private Spinner<Integer> searchToMinuteSpinner;
    @FXML private TextField searchMaxResults;
    @FXML private TextField searchValueDeserializerField;
    @FXML private CheckBox decodeAsFlatbufCheck;
    @FXML private TextField searchFlatbufClassNameField;
    @FXML private TextField sourceKeyDeserializerField;
    @FXML private TextArea searchResultsArea;
    @FXML private Label searchStatusLabel;
    @FXML private Button searchButton;
    @FXML private Button stopSearchButton;
    @FXML private Button peekOneButton;
    @FXML private ComboBox<String> configsCombo;
    @FXML private HBox searchFromBeginningBox;
    @FXML private CheckBox searchFromBeginningCheck;
    @FXML private Button startTailButton;
    @FXML private Label metricsHeapLabel;
    @FXML private Label metricsThreadsLabel;
    @FXML private Label metricsCpuLabel;
    @FXML private Label metricsGcLabel;

    // --- FXML Fields: Push Tab ---
    @FXML private TextField sourceTopicField;
    @FXML private TextField sourcePartitionField;
    @FXML private ComboBox<String> pushTopicComboBox;
    @FXML private ComboBox<String> targetPartitionField;
    @FXML private TextField messageCountField;
    @FXML private TextField threadPoolSizeField;
    @FXML private TextField batchSizeField;
    @FXML private TextArea messageTemplateArea;
    @FXML private TextField keySerializerField;
    @FXML private TextField valueSerializerField;
    @FXML private TextField protobufClassNameField;
    @FXML private TextField pushFlatbufClassNameField;
    @FXML private CheckBox pushFlatbufModeCheck;
    @FXML private TextField messageHeadersField;
    @FXML private TextField autoIncrementFieldsField;
    @FXML private TextField rotationFieldsField;
    @FXML private TextField uuidFieldsField;
    @FXML private TextArea pushMessageTemplateArea;
    @FXML private ComboBox<String> autoIncrementFieldPicker;
    @FXML private TextField autoIncrementStartField;
    @FXML private TextField autoIncrementStepField;
    @FXML private ComboBox<String> rotationFieldPicker;
    @FXML private TextField rotationValuesField;
    @FXML private ComboBox<String> uuidFieldPicker;
    @FXML private ComboBox<String> headerKeyField;
    @FXML private ComboBox<String> headerFieldPicker;
    @FXML private RadioButton partitionKeyAll;
    @FXML private RadioButton partitionKeySpecific;
    @FXML private RadioButton partitionKeyField;
    @FXML private RadioButton partitionKeyConstant;
    @FXML private ComboBox<String> keyFieldComboBox;
    @FXML private TextField keyConstantField;
    @FXML private Button startButton;
    @FXML private Button stopButton;
    @FXML private ProgressBar progressBar;
    @FXML private Label statusLabel;

    // --- FXML Fields: Verify Tab ---
    @FXML private ComboBox<String> verifyTopicComboBox;
    @FXML private ComboBox<String> availableConsumerGroupsCombo;
    @FXML private ListView<String> selectedConsumerGroupsList;
    @FXML private TextArea verifyPartitionOffsetsArea;
    @FXML private TextField verifyTimeoutField;
    @FXML private Button verifyStartButton;
    @FXML private Button verifyStopButton;
    @FXML private Label verifyStatusLabel;

    // --- FXML Fields: Topic Management Tab ---
    // Section 1: Describe
    @FXML private ComboBox<String> describeTopicComboBox;
    @FXML private Label describeTopicStatusLabel;

    // Section 2: Update
    @FXML private ComboBox<String> updateTopicComboBox;
    @FXML private ListView<String> topicConfigNamesList;
    @FXML private TextArea updateTopicConfigsArea;
    @FXML private Label updateTopicStatusLabel;

    // Section 3: Create
    @FXML private TextField newTopicNameField;
    @FXML private ListView<String> createTopicConfigNamesList;
    @FXML private TextField newTopicPartitionsField;
    @FXML private TextField newTopicReplicationField;
    @FXML private TextArea createTopicConfigsArea;
    @FXML private Label createTopicStatusLabel;

    // Partition Offsets Table (inside Topic Details)
    @FXML private TableView<PartitionOffsetData> partitionOffsetTable;
    @FXML private TableColumn<PartitionOffsetData, Integer> partitionColumn;
    @FXML private TableColumn<PartitionOffsetData, Long> earliestOffsetColumn;
    @FXML private TableColumn<PartitionOffsetData, Long> latestOffsetColumn;
    @FXML private TableColumn<PartitionOffsetData, Long> messageCountColumn;

    // Shared
    @FXML private TextArea topicDetailsArea;
    @FXML private javafx.scene.control.TitledPane topicDetailsPane;

    // --- FXML Fields: Consumer Groups Tab ---
    // Section 1: Describe Group
    @FXML private ComboBox<String> cgTopicComboBox;
    @FXML private ComboBox<String> cgConsumerGroupComboBox;
    @FXML private Label cgDescribeStatusLabel;
    @FXML private javafx.scene.control.TitledPane cgDetailsPane;
    @FXML private TextArea cgDetailsArea;

    // Section 2: Consumer Lag
    @FXML private ComboBox<String> cgLagTopicComboBox;
    @FXML private ComboBox<String> cgLagConsumerGroupComboBox;
    @FXML private Label cgLagStatusLabel;
    @FXML private Label cgLagLiveLabel;
    @FXML private Label cgLagLastUpdatedLabel;
    @FXML private TableView<?> cgLagTable;
    @FXML private TableColumn<?, ?> cgLagBarColumn;
    @FXML private Label cgTotalLagLabel;
    @FXML private Button stopLagButton;
    @FXML private LineChart<String, Number> cgLagTrendChart;
    @FXML private CategoryAxis cgLagChartXAxis;
    @FXML private NumberAxis cgLagChartYAxis;

    // Section 3: Reset Offsets
    @FXML private ComboBox<String> cgResetGroupIdComboBox;
    @FXML private ComboBox<String> cgResetTopicComboBox;
    @FXML private ComboBox<String> cgResetStrategyCombo;
    @FXML private javafx.scene.control.DatePicker cgResetDatePicker;
    @FXML private javafx.scene.control.Spinner<Integer> cgResetHourSpinner;
    @FXML private javafx.scene.control.Spinner<Integer> cgResetMinuteSpinner;
    @FXML private TextField cgResetShiftField;
    @FXML private Label cgResetValueLabel;
    @FXML private Label cgResetValueHintLabel;
    @FXML private Label cgResetStatusLabel;

    
    // --- State ---
    private boolean consoleVisible = false;
    private boolean searchResultsVisible = false;
    private volatile boolean searchInProgress = false;
    private final java.util.concurrent.atomic.AtomicBoolean searchStopFlag = new java.util.concurrent.atomic.AtomicBoolean(false);
    // Multiple pod connections - each pod has its own AdminClient and topics
    private Map<String, org.apache.kafka.clients.admin.AdminClient> podAdminClients = new HashMap<>();
    private Set<String> connectedPods = new HashSet<>();
    private Map<String, List<String>> podTopics = new HashMap<>();  // topics per pod
    // Consumer Group File Storage: stored as per-pod JSON files in kafka-pilot/cache directory — no in-memory cache
    // File path: ./kafka-pilot/cache/cg-cache-<podName>.json (cross-platform compatible)
    // Format: { "_timestamp": 1234567890, "<topic>": [ { "groupId": "...", "topic": "...", "members": [...] }, ... ] }
    private static final long CONSUMER_GROUP_FILE_STORAGE_TTL_MS = 15 * 60 * 1000; // 15 minutes
    // Track pods currently fetching consumer groups to prevent duplicate fetches
    private final Set<String> podsFetchingConsumerGroups = ConcurrentHashMap.newKeySet();

    /**
     * Inner class to hold consumer group information for a topic
     */
    private static class ConsumerGroupInfo {
        String groupId;
        String topic;
        List<ConsumerMemberInfo> members;
        Map<String, Long> partitionLag; // partition -> lag
        Map<String, Long> committedOffset; // partition -> committed offset
        Map<String, Long> endOffset; // partition -> end offset
        long totalLag;
        long timestamp;

        ConsumerGroupInfo(String groupId, String topic) {
            this.groupId = groupId;
            this.topic = topic;
            this.members = new ArrayList<>();
            this.partitionLag = new HashMap<>();
            this.committedOffset = new HashMap<>();
            this.endOffset = new HashMap<>();
            this.timestamp = System.currentTimeMillis();
        }
    }

    /**
     * Inner class to hold consumer member information
     */
    private static class ConsumerMemberInfo {
        String memberId;
        String clientId;
        String host;
        List<String> assignedPartitions;

        ConsumerMemberInfo(String memberId, String clientId, String host) {
            this.memberId = memberId;
            this.clientId = clientId;
            this.host = host;
            this.assignedPartitions = new ArrayList<>();
        }
    }
    private java.util.List<String> allSearchTopics = new java.util.ArrayList<>();
    private java.util.List<String> allPushTopics = new java.util.ArrayList<>();
    private java.util.List<String> allVerifyTopics = new java.util.ArrayList<>();
    private boolean updatingSearchCombo = false;
    private boolean updatingPushCombo = false;
    private boolean updatingVerifyCombo = false;
    private boolean updatingDescribeCombo = false;
    private boolean updatingUpdateCombo = false;
    private boolean updatingCgDescribeCombo = false;
    private boolean updatingCgLagCombo = false;
    private boolean updatingCgResetCombo = false;
    
    // --- State: Verify tab ---
    private final List<KafkaConsumerVerifier> activeVerifiers = new ArrayList<>();

    // --- State: Broker Management ---
    private List<KafkaBrokerConfig> brokerConfigs = new ArrayList<>();
    private KafkaBrokerConfig selectedBroker = null;  // currently connected broker
    private KafkaBrokerConfig dropdownSelectedBroker = null;  // broker selected in dropdown
    private String currentPodName = null;
    private Map<String, PodState> podStateMap = new HashMap<>();

    // --- Services ---
    private KafkaLoadTestEngine engine;
    private TestMetrics metrics;
    private KafkaSearchService searchService;
    private KafkaTailService tailService;
    private ConfigurationManager configManager;
    private ExportService exportService;
    private MavenDependencyResolver mavenResolver;
    private ScheduledExecutorService metricsScheduler;

    // --- Console Logging: Batched with Auto-Clear ---
    private static final int CONSOLE_BATCH_SIZE = 100;        // Flush every N messages
    private static final int CONSOLE_MAX_LINES = 10000;       // Auto-clear threshold
    private static final int CONSOLE_FLUSH_INTERVAL_MS = 100; // Flush every 100ms
    private final java.util.concurrent.ConcurrentLinkedQueue<String> consoleLogQueue = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final java.util.concurrent.atomic.AtomicBoolean consoleFlushInProgress = new java.util.concurrent.atomic.AtomicBoolean(false);
    private volatile boolean consoleAutoFlushEnabled = true;
    private ScheduledExecutorService consoleFlushScheduler;

    // --- In-Memory Storage: Last Search Results ---
    // Stores partition -> max offset for quick access even when UI is frozen
    private final java.util.concurrent.ConcurrentHashMap<Integer, Long> lastSearchPartitionOffsets = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile String lastSearchTopic = null;
    private final java.util.concurrent.atomic.AtomicReference<String> lastSearchMessageTemplate = new java.util.concurrent.atomic.AtomicReference<>();

    // --- State: Live Tail ---
    private volatile boolean tailInProgress = false;
    private final java.util.concurrent.atomic.AtomicBoolean tailStopFlag = new java.util.concurrent.atomic.AtomicBoolean(false);

    // --- State: Live Lag ---
    private ScheduledExecutorService lagScheduler;
    private XYChart.Series<String, Number> lagTrendSeries;
    private static final int LAG_CHART_MAX_POINTS = 60; // 5 min at 5s interval
    private volatile boolean consumerGroupsLoading = false;

    // ========== INITIALIZATION ==========

    @FXML
    public void initialize() {
        logger.info("Initializing MainController");

        searchService = new KafkaSearchService();
        tailService = new KafkaTailService();
        configManager = new ConfigurationManager();
        exportService = new ExportService();
        metrics = new TestMetrics();
        mavenResolver = new MavenDependencyResolver();

        stopButton.setDisable(true);
        progressBar.setProgress(0);

        initSearchControls();
        initTopicFiltering();
        initConsumerGroupLists();
        initBrokerControls();
        initSslControls();  // Initialize SSL field enable/disable based on checkbox
        initTopicConfigNamesList();  // Initialize click-to-copy for topic configs
        initOffsetTable();  // Initialize partition offset table
        startMetricsTicker();
        startConsoleFlushScheduler();  // Start batched console logging
        loadBrokerConfigs();
        // Pre-load all pod states into memory from last saved config (no UI changes)
        podStateMap.putAll(configManager.loadPodStatesFromDisk());
        loadDefaultConfiguration();
        refreshAllConfigs();

        // Auto-load first saved configuration if available
        List<String> savedConfigs = configManager.listConfigurations();
        if (!savedConfigs.isEmpty()) {
            String firstConfig = savedConfigs.get(0);
            try {
                TestConfiguration config = configManager.loadConfiguration(firstConfig);
                applyFullConfiguration(config);
                configsCombo.setValue(firstConfig);
                appendToConsole("[Config] Auto-loaded: " + firstConfig);
            } catch (Exception e) {
                logger.warn("[Config] Failed to auto-load config: {}", firstConfig, e);
            }
        }

        logger.info("MainController initialized");
        appendToConsole("Ready. Config: " + configManager.getConfigDirectory());
    }

    // ========== UI INITIALIZATION ==========
    
    private void updateFromBeginningVisibility() {
        boolean timeRangeActive = searchWithTimeRange.isSelected();
        boolean specificPartitionSelected = searchSpecificPartition.isSelected();
        String offsetText = sourceOffsetField.getText();
        boolean offsetFilled = offsetText != null && !offsetText.trim().isEmpty();
        boolean show = !timeRangeActive && !(specificPartitionSelected && offsetFilled);
        if (!show) searchFromBeginningCheck.setSelected(false);
        searchFromBeginningBox.setVisible(show);
        searchFromBeginningBox.setManaged(show);
    }

    private void initSearchControls() {
        searchSpecificPartition.selectedProperty().addListener((obs, o, nv) -> {
            searchPartitionField.setDisable(!nv);
            sourceOffsetField.setDisable(!nv);
            offsetLabel.setDisable(!nv);
            updateFromBeginningVisibility();
        });
        searchWithTimeRange.selectedProperty().addListener((obs, o, nv) -> {
            searchFromTimeBox.setDisable(!nv);
            searchToTimeBox.setDisable(!nv);
            updateFromBeginningVisibility();
        });
        searchNoTimeRange.selectedProperty().addListener((obs, o, nv) -> updateFromBeginningVisibility());
        sourceOffsetField.textProperty().addListener((obs, o, nv) -> updateFromBeginningVisibility());

        decodeAsFlatbufCheck.selectedProperty().addListener((obs, o, flatbuf) -> {
            // Only disable key/value deserializers - FlatBuffer uses its own class-based decoding
            sourceKeyDeserializerField.setDisable(flatbuf);
            searchValueDeserializerField.setDisable(flatbuf);
        });

        pushFlatbufModeCheck.selectedProperty().addListener((obs, o, flatbuf) -> {
            keySerializerField.setDisable(flatbuf);
            valueSerializerField.setDisable(flatbuf);
            protobufClassNameField.setDisable(flatbuf);
        });

        partitionKeySpecific.selectedProperty().addListener((obs, o, nv) -> {
            targetPartitionField.setDisable(!nv);
        });

        partitionKeyField.selectedProperty().addListener((obs, o, nv) -> {
            keyFieldComboBox.setDisable(!nv);
            if (nv) {
                populateKeyFieldComboBox();
            }
        });

        partitionKeyConstant.selectedProperty().addListener((obs, o, nv) -> {
            keyConstantField.setDisable(!nv);
        });

        pushMessageTemplateArea.textProperty().addListener((obs, o, nv) -> {
            populateKeyFieldComboBox();
        });

        searchFromHourSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 23, 0));
        searchFromMinuteSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 59, 0));
        searchToHourSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 23, 23));
        searchToMinuteSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 59, 59));
        searchFromDatePicker.setValue(java.time.LocalDate.now());
        searchToDatePicker.setValue(java.time.LocalDate.now());

        // Reset offset datetime spinners
        cgResetHourSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 23, 0));
        cgResetMinuteSpinner.setValueFactory(new SpinnerValueFactory.IntegerSpinnerValueFactory(0, 59, 0));
        cgResetDatePicker.setValue(java.time.LocalDate.now());

        // Populate partition dropdowns when a topic is selected
        searchTopicComboBox.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null && !newVal.trim().isEmpty()) {
                fetchPartitionsForTopic(newVal.trim(), searchPartitionField);
            }
        });
        pushTopicComboBox.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null && !newVal.trim().isEmpty()) {
                fetchPartitionsForTopic(newVal.trim(), targetPartitionField);
            }
        });
        verifyTopicComboBox.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal != null && !newVal.trim().isEmpty()) {
                // Clear consumer groups when topic changes
                availableConsumerGroupsCombo.getItems().clear();
                selectedConsumerGroupsList.getItems().clear();
                updateVerifyStatusLabel();
            }
        });

        // Reset Offset strategy listener - updates label and hint based on strategy
        cgResetStrategyCombo.valueProperty().addListener((obs, oldVal, newVal) -> {
            if (newVal == null) return;
            switch (newVal) {
                case "to-datetime":
                    cgResetValueLabel.setText("Date/Time:");
                    cgResetValueHintLabel.setText("Select date and time to reset to");
                    cgResetDatePicker.setDisable(false);
                    cgResetHourSpinner.setDisable(false);
                    cgResetMinuteSpinner.setDisable(false);
                    cgResetShiftField.setDisable(true);
                    break;
                case "shift-by":
                    cgResetValueLabel.setText("Date/Time:");
                    cgResetValueHintLabel.setText("Enter shift value below");
                    cgResetDatePicker.setDisable(true);
                    cgResetHourSpinner.setDisable(true);
                    cgResetMinuteSpinner.setDisable(true);
                    cgResetShiftField.setDisable(false);
                    break;
                case "earliest":
                case "latest":
                    cgResetValueLabel.setText("Date/Time:");
                    cgResetValueHintLabel.setText("No date needed for earliest/latest strategy");
                    cgResetDatePicker.setDisable(true);
                    cgResetHourSpinner.setDisable(true);
                    cgResetMinuteSpinner.setDisable(true);
                    cgResetShiftField.setDisable(true);
                    break;
                default:
                    cgResetValueLabel.setText("Date/Time:");
                    cgResetValueHintLabel.setText("");
                    cgResetDatePicker.setDisable(false);
                    cgResetHourSpinner.setDisable(false);
                    cgResetMinuteSpinner.setDisable(false);
                    cgResetShiftField.setDisable(true);
            }
        });

        // Set up window close handler with confirmation
        Platform.runLater(() -> {
            if (consoleOutput.getScene() != null && consoleOutput.getScene().getWindow() != null) {
                javafx.stage.Stage stage = (javafx.stage.Stage) consoleOutput.getScene().getWindow();
                stage.setOnCloseRequest(event -> {
                    event.consume(); // Prevent default close
                    handleClose();
                });
            }
        });
    }

    private void initTopicFiltering() {
        // Search topic ComboBox filtering
        searchTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingSearchCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allSearchTopics.isEmpty()) {
                    updatingSearchCombo = true;
                    searchTopicComboBox.getItems().setAll(allSearchTopics);
                    updatingSearchCombo = false;
                }
            } else {
                filterComboBoxSafely(searchTopicComboBox, allSearchTopics, newVal, updatingSearchCombo);
            }
        });

        // Push topic ComboBox filtering
        pushTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingPushCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allPushTopics.isEmpty()) {
                    updatingPushCombo = true;
                    pushTopicComboBox.getItems().setAll(allPushTopics);
                    updatingPushCombo = false;
                }
            } else {
                filterComboBoxSafely(pushTopicComboBox, allPushTopics, newVal, updatingPushCombo);
            }
        });

        // Verify topic ComboBox filtering
        verifyTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingVerifyCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allVerifyTopics.isEmpty()) {
                    updatingVerifyCombo = true;
                    verifyTopicComboBox.getItems().setAll(allVerifyTopics);
                    updatingVerifyCombo = false;
                }
            } else {
                filterComboBoxSafely(verifyTopicComboBox, allVerifyTopics, newVal, updatingVerifyCombo);
            }
        });

        // Topics tab - Describe topic ComboBox filtering
        describeTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingDescribeCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allSearchTopics.isEmpty()) {
                    updatingDescribeCombo = true;
                    describeTopicComboBox.getItems().setAll(allSearchTopics);
                    updatingDescribeCombo = false;
                }
            } else {
                filterComboBoxSafely(describeTopicComboBox, allSearchTopics, newVal, updatingDescribeCombo);
            }
        });

        // Topics tab - Update topic ComboBox filtering
        updateTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingUpdateCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allSearchTopics.isEmpty()) {
                    updatingUpdateCombo = true;
                    updateTopicComboBox.getItems().setAll(allSearchTopics);
                    updatingUpdateCombo = false;
                }
            } else {
                filterComboBoxSafely(updateTopicComboBox, allSearchTopics, newVal, updatingUpdateCombo);
            }
        });

        // Consumer Groups tab - Describe ComboBox filtering
        cgTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingCgDescribeCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allSearchTopics.isEmpty()) {
                    updatingCgDescribeCombo = true;
                    cgTopicComboBox.getItems().setAll(allSearchTopics);
                    updatingCgDescribeCombo = false;
                }
            } else {
                filterComboBoxSafely(cgTopicComboBox, allSearchTopics, newVal, updatingCgDescribeCombo);
            }
        });

        // Consumer Groups tab - Lag ComboBox filtering
        cgLagTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingCgLagCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allSearchTopics.isEmpty()) {
                    updatingCgLagCombo = true;
                    cgLagTopicComboBox.getItems().setAll(allSearchTopics);
                    updatingCgLagCombo = false;
                }
            } else {
                filterComboBoxSafely(cgLagTopicComboBox, allSearchTopics, newVal, updatingCgLagCombo);
            }
        });

        // Consumer Groups tab - Reset Offset ComboBox filtering
        cgResetTopicComboBox.getEditor().textProperty().addListener((obs, oldVal, newVal) -> {
            if (updatingCgResetCombo) return;
            if (newVal == null || newVal.isEmpty()) {
                if (!allSearchTopics.isEmpty()) {
                    updatingCgResetCombo = true;
                    cgResetTopicComboBox.getItems().setAll(allSearchTopics);
                    updatingCgResetCombo = false;
                }
            } else {
                filterComboBoxSafely(cgResetTopicComboBox, allSearchTopics, newVal, updatingCgResetCombo);
            }
        });

            }

    private void initConsumerGroupLists() {
        // Click on selected list -> remove item
        selectedConsumerGroupsList.setOnMouseClicked(event -> {
            String selected = selectedConsumerGroupsList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                selectedConsumerGroupsList.getItems().remove(selected);
                // Add back to available if not already there
                if (!availableConsumerGroupsCombo.getItems().contains(selected)) {
                    availableConsumerGroupsCombo.getItems().add(selected);
                }
                updateVerifyStatusLabel();
            }
        });
    }

    private void updateVerifyStatusLabel() {
        int count = selectedConsumerGroupsList.getItems().size();
        if (count == 0) {
            verifyStatusLabel.setText("Select at least one consumer group");
        } else {
            verifyStatusLabel.setText(count + " group(s) selected");
        }
    }

    // ========== BROKER MANAGEMENT ==========
    
    private void initBrokerControls() {
        // Selected for Work is the single source of truth for all operations
        selectedForWorkComboBox.getSelectionModel().selectedItemProperty().addListener((obs, oldVal, newVal) -> {
            // Save current pod state before switching
            if (currentPodName != null && !currentPodName.isEmpty()) {
                saveCurrentPodState(currentPodName);
            }

            dropdownSelectedBroker = newVal;

            // Always clear all fields and results immediately on pod switch
            clearAllFieldsToDefault();
            clearAllResults();

            if (newVal != null) {
                brokerNameField.setText(newVal.getName());
                brokerServersField.setText(newVal.getBootstrapServers());
                // Auto-fill maven dependency across all tabs
                if (newVal.getMavenDependency() != null && !newVal.getMavenDependency().isEmpty()) {
                    mavenDependencyField.setText(newVal.getMavenDependency());
                } else {
                    mavenDependencyField.setText("");
                }
                String defaultTopic = newVal.getDefaultTopicName();
                if (defaultTopic != null && !defaultTopic.isEmpty()) {
                    searchTopicComboBox.setValue(defaultTopic);
                    pushTopicComboBox.setValue(defaultTopic);
                    verifyTopicComboBox.setValue(defaultTopic);
                } else {
                    searchTopicComboBox.setValue(null);
                    pushTopicComboBox.setValue(null);
                    verifyTopicComboBox.setValue(null);
                }
                // Load SSL configuration for this broker
                loadSslConfigToUI(newVal);
                String podName = newVal.getName();
                // Restore this pod's saved state (search, push, verify fields)
                loadPodState(podName);
                // Restore topics for this pod (if available)
                updateTopicsForCurrentPod(podName);
                currentPodName = podName;
                appendToConsole("[Selected for Work] Changed to: " + podName);
            } else {
                currentPodName = null;
                clearSslFields();
            }

            updateConnectButtonState();
        });
    }

    /**
     * Initializes SSL controls - enables/disables SSL fields based on checkbox.
     */
    private void initSslControls() {
        if (sslEnabledCheckBox == null) return;

        // Listener to enable/disable SSL fields based on checkbox
        sslEnabledCheckBox.selectedProperty().addListener((obs, oldVal, newVal) -> {
            boolean enabled = newVal;

            // Security protocol combo
            if (sslSecurityProtocolCombo != null) {
                sslSecurityProtocolCombo.setDisable(!enabled);
            }

            // Truststore fields
            if (sslTruststoreLocationField != null) {
                sslTruststoreLocationField.setDisable(!enabled);
            }
            if (sslTruststorePasswordField != null) {
                sslTruststorePasswordField.setDisable(!enabled);
            }
            if (sslTruststoreTypeCombo != null) {
                sslTruststoreTypeCombo.setDisable(!enabled);
            }

            // Keystore fields
            if (sslKeystoreLocationField != null) {
                sslKeystoreLocationField.setDisable(!enabled);
            }
            if (sslKeystorePasswordField != null) {
                sslKeystorePasswordField.setDisable(!enabled);
            }
            if (sslKeyPasswordField != null) {
                sslKeyPasswordField.setDisable(!enabled);
            }
            if (sslKeystoreTypeCombo != null) {
                sslKeystoreTypeCombo.setDisable(!enabled);
            }

            // Endpoint algorithm combo
            if (sslEndpointAlgorithmCombo != null) {
                sslEndpointAlgorithmCombo.setDisable(!enabled);
            }

            // Update status label
            updateSslStatusLabel();
        });

        // Initial state - disabled by default
        boolean initiallyEnabled = sslEnabledCheckBox.isSelected();
        if (sslSecurityProtocolCombo != null) sslSecurityProtocolCombo.setDisable(!initiallyEnabled);
        if (sslTruststoreLocationField != null) sslTruststoreLocationField.setDisable(!initiallyEnabled);
        if (sslTruststorePasswordField != null) sslTruststorePasswordField.setDisable(!initiallyEnabled);
        if (sslTruststoreTypeCombo != null) sslTruststoreTypeCombo.setDisable(!initiallyEnabled);
        if (sslKeystoreLocationField != null) sslKeystoreLocationField.setDisable(!initiallyEnabled);
        if (sslKeystorePasswordField != null) sslKeystorePasswordField.setDisable(!initiallyEnabled);
        if (sslKeyPasswordField != null) sslKeyPasswordField.setDisable(!initiallyEnabled);
        if (sslKeystoreTypeCombo != null) sslKeystoreTypeCombo.setDisable(!initiallyEnabled);
        if (sslEndpointAlgorithmCombo != null) sslEndpointAlgorithmCombo.setDisable(!initiallyEnabled);
    }

    /**
     * Initializes the partition offset table.
     */
    private void initOffsetTable() {
        // Initialize table columns
        partitionColumn.setCellValueFactory(cellData -> 
            new javafx.beans.property.SimpleIntegerProperty(cellData.getValue().getPartition()).asObject());
        earliestOffsetColumn.setCellValueFactory(cellData -> 
            new javafx.beans.property.SimpleLongProperty(cellData.getValue().getEarliestOffset()).asObject());
        latestOffsetColumn.setCellValueFactory(cellData -> 
            new javafx.beans.property.SimpleLongProperty(cellData.getValue().getLatestOffset()).asObject());
        messageCountColumn.setCellValueFactory(cellData -> 
            new javafx.beans.property.SimpleLongProperty(cellData.getValue().getMessageCount()).asObject());
    }

    /**
     * Updates the topic lists for the specified pod (restores from cache or clears if not connected).
     */
    private void updateTopicsForCurrentPod(String podName) {
        List<String> topics = podTopics.get(podName);
        if (topics != null && !topics.isEmpty()) {
            // Restore this pod's topics
            allSearchTopics.clear();
            allSearchTopics.addAll(topics);
            allPushTopics.clear();
            allPushTopics.addAll(topics);
            allVerifyTopics.clear();
            allVerifyTopics.addAll(topics);
            searchTopicComboBox.getItems().setAll(topics);
            pushTopicComboBox.getItems().setAll(topics);
            verifyTopicComboBox.getItems().setAll(topics);
            // Also refresh Topics tab dropdowns for this pod
            refreshTopicsTabDropdowns();
            // Also refresh Consumer Groups tab dropdowns
            refreshConsumerGroupsTabDropdowns();
        } else {
            // Clear topics if pod has no cached topics
            searchTopicComboBox.getItems().clear();
            pushTopicComboBox.getItems().clear();
            verifyTopicComboBox.getItems().clear();
            // Clear Topics tab dropdowns too
            Platform.runLater(() -> {
                if (describeTopicComboBox != null) describeTopicComboBox.getItems().clear();
                if (updateTopicComboBox != null) updateTopicComboBox.getItems().clear();
                // Clear Consumer Groups tab dropdowns
                if (cgTopicComboBox != null) cgTopicComboBox.getItems().clear();
                if (cgLagTopicComboBox != null) cgLagTopicComboBox.getItems().clear();
                if (cgResetTopicComboBox != null) cgResetTopicComboBox.getItems().clear();
            });
        }
    }

    /**
     * Updates the Connect button text and style based on whether the
     * dropdown-selected pod is connected.
     */
    private void updateConnectButtonState() {
        if (dropdownSelectedBroker != null && isPodConnected(dropdownSelectedBroker.getName())) {
            // Selected pod is connected - show Disconnect
            connectButton.setText("Disconnect");
            connectButton.setStyle("-fx-background-color: #e74c3c; -fx-text-fill: white; -fx-font-size: 11px;");
        } else {
            // Not connected - show Connect
            connectButton.setText("Connect");
            connectButton.setStyle("-fx-background-color: #27ae60; -fx-text-fill: white; -fx-font-size: 11px;");
        }
    }

    private void loadBrokerConfigs() {
        brokerConfigs = configManager.loadBrokerConfigs();
        refreshBrokerComboBoxes();
    }

    private void refreshBrokerComboBoxes() {
        selectedForWorkComboBox.getItems().setAll(brokerConfigs);
    }

    private void saveBrokerConfigs() {
        try {
            configManager.saveBrokerConfigs(brokerConfigs);
        } catch (Exception e) {
            handleError("Broker", "Error saving broker configs", e);
        }
    }

    private KafkaBrokerConfig getSelectedBroker() {
        return selectedForWorkComboBox != null ? selectedForWorkComboBox.getValue() : null;
    }

    /**
     * Sets the active (connected) pod and handles state switching.
     * This is called when Connect button is clicked.
     */
    private void setActivePod(KafkaBrokerConfig broker) {
        // Save current pod state before switching
        if (currentPodName != null && !currentPodName.isEmpty()) {
            saveCurrentPodState(currentPodName);
        }

        // Set new broker as active
        selectedBroker = broker;

        // Update Active section UI (just pod names, comma-separated)
        if (broker != null) {
            activePodsField.setText(broker.getName());
            currentPodName = broker.getName();
            // Load the new pod's state
            loadPodState(currentPodName);
            appendToConsole("[Broker] Connected: " + broker.getName());
        } else {
            activePodsField.setText("");
            currentPodName = null;
            clearAllFieldsToDefault();
        }

        // Update connect button state
        updateConnectButtonState();

        // Clear all results when switching pods
        clearAllResults();
    }

    private String getCurrentPodName() {
        return (currentPodName != null && !currentPodName.isEmpty()) ? currentPodName : "UNKNOWN";
    }

    /**
     * Saves the current state of all UI fields for the specified pod.
     */
    private void saveCurrentPodState(String podName) {
        PodState state = PodState.builder()
                // Search Tab
                .searchTopic(searchTopicComboBox.getValue())
                .searchTextFilter(searchTextFilter.getText())
                .searchAllPartitions(searchAllPartitions.isSelected())
                .searchSpecificPartitionValue(searchPartitionField.getValue())
                .sourceOffset(sourceOffsetField.getText())
                .searchNoTimeRange(searchNoTimeRange.isSelected())
                .searchFromDate(searchFromDatePicker.getValue() != null ? searchFromDatePicker.getValue().toString() : "")
                .searchFromHour(searchFromHourSpinner.getValue() != null ? searchFromHourSpinner.getValue().toString() : "0")
                .searchFromMinute(searchFromMinuteSpinner.getValue() != null ? searchFromMinuteSpinner.getValue().toString() : "0")
                .searchToDate(searchToDatePicker.getValue() != null ? searchToDatePicker.getValue().toString() : "")
                .searchToHour(searchToHourSpinner.getValue() != null ? searchToHourSpinner.getValue().toString() : "0")
                .searchToMinute(searchToMinuteSpinner.getValue() != null ? searchToMinuteSpinner.getValue().toString() : "0")
                .searchMaxResults(searchMaxResults.getText())
                .searchValueDeserializer(searchValueDeserializerField.getText())
                .searchFlatbufClassName(searchFlatbufClassNameField.getText())
                .decodeAsFlatbuf(decodeAsFlatbufCheck.isSelected())
                .sourceKeyDeserializer(sourceKeyDeserializerField.getText())
                // Push Tab
                .sourceTopic(sourceTopicField.getText())
                .sourcePartition(sourcePartitionField.getText())
                .pushTopic(pushTopicComboBox.getValue())
                .targetPartition(targetPartitionField.getValue())
                .messageCount(messageCountField.getText())
                .threadPoolSize(threadPoolSizeField.getText())
                .batchSize(batchSizeField.getText())
                .messageTemplate(messageTemplateArea.getText())
                .keySerializer(keySerializerField.getText())
                .valueSerializer(valueSerializerField.getText())
                .protobufClassName(protobufClassNameField.getText())
                .pushFlatbufClassName(pushFlatbufClassNameField.getText())
                .pushFlatbufMode(pushFlatbufModeCheck.isSelected())
                .messageHeaders(messageHeadersField.getText())
                .autoIncrementFields(autoIncrementFieldsField.getText())
                .rotationFields(rotationFieldsField.getText())
                .uuidFields(uuidFieldsField.getText())
                .pushMessageTemplate(pushMessageTemplateArea.getText())
                .autoIncrementStart(autoIncrementStartField.getText())
                .autoIncrementStep(autoIncrementStepField.getText())
                .rotationValues(rotationValuesField.getText())
                .partitionKeyMode(partitionKeyAll.isSelected() ? "all" :
                        partitionKeySpecific.isSelected() ? "specific" :
                                partitionKeyField.isSelected() ? "field" : "constant")
                .partitionKeyField(keyFieldComboBox.getValue())
                .partitionKeyConstant(keyConstantField.getText())
                // Verify Tab
                .verifyTopic(verifyTopicComboBox.getValue())
                .verifyTimeout(verifyTimeoutField.getText())
                .verifyPartitionOffsets(verifyPartitionOffsetsArea.getText())
                .build();

        podStateMap.put(podName, state);
        logger.debug("Saved state for pod: {}", podName);
    }

    /**
     * Loads the saved state for the specified pod into all UI fields.
     * If no saved state exists, fields are cleared to defaults.
     */
    private void loadPodState(String podName) {
        PodState state = podStateMap.get(podName);
        if (state == null) {
            logger.debug("No saved state for pod: {}, using defaults", podName);
            clearAllFieldsToDefault();
            return;
        }

        logger.debug("Loading state for pod: {}", podName);

        // IMPORTANT: Clear all fields first before loading new state
        // This ensures null values in the new state properly clear the UI
        clearAllFieldsToDefault();

        // Search Tab
        if (state.getSearchTopic() != null) searchTopicComboBox.setValue(state.getSearchTopic());
        if (state.getSearchTextFilter() != null) searchTextFilter.setText(state.getSearchTextFilter());
        if (state.isSearchAllPartitions()) {
            searchAllPartitions.setSelected(true);
        } else {
            searchSpecificPartition.setSelected(true);
        }
        if (state.getSearchSpecificPartitionValue() != null) searchPartitionField.setValue(state.getSearchSpecificPartitionValue());
        if (state.getSourceOffset() != null) sourceOffsetField.setText(state.getSourceOffset());
        if (state.isSearchNoTimeRange()) {
            searchNoTimeRange.setSelected(true);
        } else {
            searchWithTimeRange.setSelected(true);
        }
        if (state.getSearchFromDate() != null && !state.getSearchFromDate().isEmpty()) {
            searchFromDatePicker.setValue(java.time.LocalDate.parse(state.getSearchFromDate()));
        }
        if (state.getSearchFromHour() != null) searchFromHourSpinner.getValueFactory().setValue(Integer.parseInt(state.getSearchFromHour()));
        if (state.getSearchFromMinute() != null) searchFromMinuteSpinner.getValueFactory().setValue(Integer.parseInt(state.getSearchFromMinute()));
        if (state.getSearchToDate() != null && !state.getSearchToDate().isEmpty()) {
            searchToDatePicker.setValue(java.time.LocalDate.parse(state.getSearchToDate()));
        }
        if (state.getSearchToHour() != null) searchToHourSpinner.getValueFactory().setValue(Integer.parseInt(state.getSearchToHour()));
        if (state.getSearchToMinute() != null) searchToMinuteSpinner.getValueFactory().setValue(Integer.parseInt(state.getSearchToMinute()));
        if (state.getSearchMaxResults() != null) searchMaxResults.setText(state.getSearchMaxResults());
        if (state.getSearchValueDeserializer() != null) searchValueDeserializerField.setText(state.getSearchValueDeserializer());
        if (state.getSearchFlatbufClassName() != null) searchFlatbufClassNameField.setText(state.getSearchFlatbufClassName());
        decodeAsFlatbufCheck.setSelected(state.isDecodeAsFlatbuf());
        if (state.getSourceKeyDeserializer() != null) sourceKeyDeserializerField.setText(state.getSourceKeyDeserializer());

        // Push Tab
        if (state.getSourceTopic() != null) sourceTopicField.setText(state.getSourceTopic());
        if (state.getSourcePartition() != null) sourcePartitionField.setText(state.getSourcePartition());
        if (state.getPushTopic() != null) pushTopicComboBox.setValue(state.getPushTopic());
        if (state.getTargetPartition() != null) targetPartitionField.setValue(state.getTargetPartition());
        if (state.getMessageCount() != null) messageCountField.setText(state.getMessageCount());
        if (state.getThreadPoolSize() != null) threadPoolSizeField.setText(state.getThreadPoolSize());
        if (state.getBatchSize() != null) batchSizeField.setText(state.getBatchSize());
        if (state.getMessageTemplate() != null) messageTemplateArea.setText(state.getMessageTemplate());
        if (state.getKeySerializer() != null) keySerializerField.setText(state.getKeySerializer());
        if (state.getValueSerializer() != null) valueSerializerField.setText(state.getValueSerializer());
        if (state.getProtobufClassName() != null) protobufClassNameField.setText(state.getProtobufClassName());
        if (state.getPushFlatbufClassName() != null) pushFlatbufClassNameField.setText(state.getPushFlatbufClassName());
        pushFlatbufModeCheck.setSelected(state.isPushFlatbufMode());
        if (state.getMessageHeaders() != null) messageHeadersField.setText(state.getMessageHeaders());
        if (state.getAutoIncrementFields() != null) autoIncrementFieldsField.setText(state.getAutoIncrementFields());
        if (state.getRotationFields() != null) rotationFieldsField.setText(state.getRotationFields());
        if (state.getUuidFields() != null) uuidFieldsField.setText(state.getUuidFields());
        if (state.getPushMessageTemplate() != null) pushMessageTemplateArea.setText(state.getPushMessageTemplate());
        if (state.getAutoIncrementStart() != null) autoIncrementStartField.setText(state.getAutoIncrementStart());
        if (state.getAutoIncrementStep() != null) autoIncrementStepField.setText(state.getAutoIncrementStep());
        if (state.getRotationValues() != null) rotationValuesField.setText(state.getRotationValues());

        // Partition key mode
        if ("all".equals(state.getPartitionKeyMode())) {
            partitionKeyAll.setSelected(true);
        } else if ("specific".equals(state.getPartitionKeyMode())) {
            partitionKeySpecific.setSelected(true);
        } else if ("field".equals(state.getPartitionKeyMode())) {
            partitionKeyField.setSelected(true);
        } else if ("constant".equals(state.getPartitionKeyMode())) {
            partitionKeyConstant.setSelected(true);
        }
        if (state.getPartitionKeyField() != null) keyFieldComboBox.setValue(state.getPartitionKeyField());
        if (state.getPartitionKeyConstant() != null) keyConstantField.setText(state.getPartitionKeyConstant());

        // Verify Tab
        if (state.getVerifyTopic() != null) verifyTopicComboBox.setValue(state.getVerifyTopic());
        if (state.getVerifyTimeout() != null) verifyTimeoutField.setText(state.getVerifyTimeout());
        if (state.getVerifyPartitionOffsets() != null) verifyPartitionOffsetsArea.setText(state.getVerifyPartitionOffsets());
    }

    /**
     * Clears all result areas when switching pods.
     */
    private void clearAllResults() {
        searchResultsArea.clear();
        searchResultsVisible = false;
        searchStatusLabel.setText("Results cleared - pod switched");

        // Clear console output (optional - could keep it for history)
        // consoleOutput.clear();

        // Clear verify results
        verifyStatusLabel.setText("Results cleared - pod switched");

        // Clear push status
        statusLabel.setText("Ready");
        progressBar.setProgress(0);

        // Clear selected consumer groups
        selectedConsumerGroupsList.getItems().clear();
        availableConsumerGroupsCombo.setValue(null);

        // Clear Topics Tab - Describe section
        if (describeTopicComboBox != null) {
            describeTopicComboBox.setValue(null);
            describeTopicComboBox.getItems().clear();
        }
        if (topicDetailsArea != null) topicDetailsArea.clear();
        if (partitionOffsetTable != null) partitionOffsetTable.getItems().clear();
        if (describeTopicStatusLabel != null) describeTopicStatusLabel.setText("Select a topic to describe");

        // Clear Topics Tab - Update section
        if (updateTopicComboBox != null) {
            updateTopicComboBox.setValue(null);
            updateTopicComboBox.getItems().clear();
        }
        if (updateTopicConfigsArea != null) updateTopicConfigsArea.clear();
        if (updateTopicStatusLabel != null) updateTopicStatusLabel.setText("Select topic and enter configs");

        // Clear Topics Tab - Create section (keep fields, just clear status)
        if (createTopicStatusLabel != null) createTopicStatusLabel.setText("Enter topic details");

        // Clear Consumer Groups Tab - Describe section
        if (cgTopicComboBox != null) {
            cgTopicComboBox.setValue(null);
            cgTopicComboBox.getItems().clear();
        }
        if (cgConsumerGroupComboBox != null) {
            cgConsumerGroupComboBox.setValue(null);
            cgConsumerGroupComboBox.getItems().clear();
        }
        if (cgDetailsArea != null) cgDetailsArea.clear();
        if (cgDescribeStatusLabel != null) cgDescribeStatusLabel.setText("Select a topic to load groups");
        if (cgDetailsPane != null) cgDetailsPane.setExpanded(false);

        // Clear Consumer Groups Tab - Lag section
        if (cgLagTopicComboBox != null) {
            cgLagTopicComboBox.setValue(null);
            cgLagTopicComboBox.getItems().clear();
        }
        if (cgLagConsumerGroupComboBox != null) {
            cgLagConsumerGroupComboBox.setValue(null);
            cgLagConsumerGroupComboBox.getItems().clear();
        }
        if (cgLagTable != null) cgLagTable.getItems().clear();
        if (cgTotalLagLabel != null) cgTotalLagLabel.setText("Total Lag: -");
        if (cgLagStatusLabel != null) cgLagStatusLabel.setText("Select a topic to load groups");

        // Clear Consumer Groups Tab - Reset Offsets section
        if (cgResetTopicComboBox != null) {
            cgResetTopicComboBox.setValue(null);
            cgResetTopicComboBox.getItems().clear();
        }
        if (cgResetGroupIdComboBox != null) {
            cgResetGroupIdComboBox.setValue(null);
            cgResetGroupIdComboBox.getItems().clear();
        }
        if (cgResetStatusLabel != null) cgResetStatusLabel.setText("Select a topic to load groups");
        
        logger.debug("All results cleared for pod switch");
    }

    /**
     * Clears all input fields to their default values.
     */
    private void clearAllFieldsToDefault() {
        // Search Tab - clear to defaults
        searchTopicComboBox.setValue(null);
        searchTextFilter.clear();
        searchAllPartitions.setSelected(true);
        searchPartitionField.setValue(null);
        sourceOffsetField.clear();
        searchNoTimeRange.setSelected(true);
        searchFromDatePicker.setValue(null);
        searchFromHourSpinner.getValueFactory().setValue(0);
        searchFromMinuteSpinner.getValueFactory().setValue(0);
        searchToDatePicker.setValue(null);
        searchToHourSpinner.getValueFactory().setValue(23);
        searchToMinuteSpinner.getValueFactory().setValue(59);
        searchMaxResults.setText("100");
        searchValueDeserializerField.setText(DEFAULT_DESERIALIZER);
        searchFlatbufClassNameField.clear();
        decodeAsFlatbufCheck.setSelected(false);
        sourceKeyDeserializerField.setText(DEFAULT_DESERIALIZER);

        // Push Tab - clear to defaults
        sourceTopicField.clear();
        sourcePartitionField.clear();
        pushTopicComboBox.setValue(null);
        targetPartitionField.setValue(null);
        messageCountField.setText("1");
        threadPoolSizeField.setText("1");
        batchSizeField.setText("1");
        messageTemplateArea.setText("{}");
        keySerializerField.setText(DEFAULT_SERIALIZER);
        valueSerializerField.setText(DEFAULT_SERIALIZER);
        protobufClassNameField.clear();
        pushFlatbufClassNameField.clear();
        pushFlatbufModeCheck.setSelected(false);
        messageHeadersField.clear();
        autoIncrementFieldsField.clear();
        rotationFieldsField.clear();
        uuidFieldsField.clear();
        pushMessageTemplateArea.clear();
        autoIncrementStartField.setText("1");
        autoIncrementStepField.setText("1");
        rotationValuesField.clear();
        partitionKeyAll.setSelected(true);
        keyFieldComboBox.setValue(null);
        keyConstantField.clear();

        // Verify Tab - clear to defaults
        verifyTopicComboBox.setValue(null);
        verifyTimeoutField.setText("10");
        verifyPartitionOffsetsArea.clear();
        selectedConsumerGroupsList.getItems().clear();

        logger.debug("All fields cleared to defaults");
    }

    @FXML
    private void handleAddBroker() {
        String name = brokerNameField.getText().trim();
        String servers = brokerServersField.getText().trim();

        if (name.isEmpty() || servers.isEmpty()) {
            appendToConsole("[Broker] ERROR: Name and servers required");
            return;
        }

        // Check for duplicate name
        if (brokerConfigs.stream().anyMatch(b -> b.getName().equals(name))) {
            appendToConsole("[Broker] ERROR: Broker name already exists: " + name);
            return;
        }

        // Save maven dependency from general field to broker config
        String mavenDep = mavenDependencyField != null ? mavenDependencyField.getText().trim() : null;

        KafkaBrokerConfig broker = KafkaBrokerConfig.builder()
                .name(name)
                .bootstrapServers(servers)
                .mavenDependency(mavenDep != null && !mavenDep.isEmpty() ? mavenDep : null)
                .build();

        // Add SSL configuration if enabled
        populateSslConfig(broker);

        brokerConfigs.add(broker);
        saveBrokerConfigs();
        refreshBrokerComboBoxes();
        selectedForWorkComboBox.setValue(broker);

        appendToConsole("[Broker] Added: " + name + (broker.getSecurityProtocol() != null ? " (SSL: " + broker.getSecurityProtocol() + ")" : ""));
    }

    @FXML
    private void handleUpdateBroker() {
        KafkaBrokerConfig selected = dropdownSelectedBroker;
        if (selected == null) {
            appendToConsole("[Broker] ERROR: No broker selected in dropdown");
            return;
        }

        String name = brokerNameField.getText().trim();
        String servers = brokerServersField.getText().trim();

        if (name.isEmpty() || servers.isEmpty()) {
            appendToConsole("[Broker] ERROR: Name and servers required");
            return;
        }

        // Check for duplicate name (excluding current)
        if (!name.equals(selected.getName()) &&
            brokerConfigs.stream().anyMatch(b -> b.getName().equals(name))) {
            appendToConsole("[Broker] ERROR: Broker name already exists: " + name);
            return;
        }

        selected.setName(name);
        selected.setBootstrapServers(servers);
        
        // Save maven dependency from general field to broker config
        String mavenDep = mavenDependencyField != null ? mavenDependencyField.getText().trim() : null;
        selected.setMavenDependency(mavenDep != null && !mavenDep.isEmpty() ? mavenDep : null);

        // Update SSL configuration
        populateSslConfig(selected);

        saveBrokerConfigs();
        refreshBrokerComboBoxes();
        // Refresh the active broker display if this is the connected pod
        if (selectedBroker == selected) {
            activePodsField.setText(selected.getName());
        }

        appendToConsole("[Broker] Updated: " + name + (selected.getSecurityProtocol() != null ? " (SSL: " + selected.getSecurityProtocol() + ")" : ""));
    }

    @FXML
    private void handleDeleteBroker() {
        KafkaBrokerConfig selected = dropdownSelectedBroker;
        if (selected == null) {
            appendToConsole("[Broker] ERROR: No broker selected in dropdown");
            return;
        }

        // Confirmation dialog
        javafx.scene.control.Alert confirm = new javafx.scene.control.Alert(
            javafx.scene.control.Alert.AlertType.CONFIRMATION,
            "Are you sure you want to delete broker '" + selected.getName() + "'?",
            javafx.scene.control.ButtonType.YES,
            javafx.scene.control.ButtonType.NO
        );
        confirm.setTitle("Delete Broker");
        confirm.setHeaderText(null);

        confirm.showAndWait().ifPresent(response -> {
            if (response == javafx.scene.control.ButtonType.YES) {
                brokerConfigs.remove(selected);
                saveBrokerConfigs();
                refreshBrokerComboBoxes();

                // Clear the input fields and dropdown
                brokerNameField.clear();
                brokerServersField.clear();
                selectedForWorkComboBox.setValue(null);
                dropdownSelectedBroker = null;

                // If we deleted the active (connected) broker, disconnect
                if (selectedBroker == selected) {
                    setActivePod(null);
                }
                updateConnectButtonState();

                appendToConsole("[Broker] Deleted: " + selected.getName());
            }
        });
    }

    // ========== SSL/TLS CONFIGURATION METHODS ==========

    /**
     * Populates SSL configuration from UI fields to a broker config.
     */
    private void populateSslConfig(KafkaBrokerConfig broker) {
        if (sslEnabledCheckBox != null && sslEnabledCheckBox.isSelected()) {
            broker.setSecurityProtocol(sslSecurityProtocolCombo.getValue());
            broker.setSslTruststoreLocation(getFieldValue(sslTruststoreLocationField));
            broker.setSslTruststorePassword(getPasswordValue(sslTruststorePasswordField));
            broker.setSslTruststoreType(sslTruststoreTypeCombo.getValue());
            broker.setSslKeystoreLocation(getFieldValue(sslKeystoreLocationField));
            broker.setSslKeystorePassword(getPasswordValue(sslKeystorePasswordField));
            broker.setSslKeyPassword(getPasswordValue(sslKeyPasswordField));
            broker.setSslKeystoreType(sslKeystoreTypeCombo.getValue());
            broker.setSslEndpointIdentificationAlgorithm(sslEndpointAlgorithmCombo.getValue());
        } else {
            // Clear SSL config if not enabled
            broker.setSecurityProtocol(null);
            broker.setSslTruststoreLocation(null);
            broker.setSslTruststorePassword(null);
            broker.setSslTruststoreType(null);
            broker.setSslKeystoreLocation(null);
            broker.setSslKeystorePassword(null);
            broker.setSslKeyPassword(null);
            broker.setSslKeystoreType(null);
            broker.setSslEndpointIdentificationAlgorithm(null);
        }
    }

    /**
     * Builds SSL properties map for Kafka producer from broker config.
     * Returns empty map if SSL is not enabled.
     */
    private Map<String, String> buildSslPropertiesForProducer(KafkaBrokerConfig broker) {
        Map<String, String> props = new HashMap<>();

        // Only add SSL properties if security protocol is set and not PLAINTEXT
        String securityProtocol = broker.getSecurityProtocol();
        if (securityProtocol == null || securityProtocol.isEmpty() || "PLAINTEXT".equals(securityProtocol)) {
            return props; // Return empty map for non-SSL connections
        }

        // Security protocol (SSL, SASL_SSL, etc.)
        props.put("security.protocol", securityProtocol);

        // Truststore configuration (required for SSL/SASL_SSL)
        if (broker.getSslTruststoreLocation() != null && !broker.getSslTruststoreLocation().isEmpty()) {
            props.put("ssl.truststore.location", broker.getSslTruststoreLocation());
            if (broker.getSslTruststorePassword() != null) {
                props.put("ssl.truststore.password", broker.getSslTruststorePassword());
            }
            if (broker.getSslTruststoreType() != null) {
                props.put("ssl.truststore.type", broker.getSslTruststoreType());
            }
        }

        // Keystore configuration (for mutual TLS)
        if (broker.getSslKeystoreLocation() != null && !broker.getSslKeystoreLocation().isEmpty()) {
            props.put("ssl.keystore.location", broker.getSslKeystoreLocation());
            if (broker.getSslKeystorePassword() != null) {
                props.put("ssl.keystore.password", broker.getSslKeystorePassword());
            }
            if (broker.getSslKeyPassword() != null) {
                props.put("ssl.key.password", broker.getSslKeyPassword());
            }
            if (broker.getSslKeystoreType() != null) {
                props.put("ssl.keystore.type", broker.getSslKeystoreType());
            }
        }

        // Endpoint identification algorithm
        if (broker.getSslEndpointIdentificationAlgorithm() != null) {
            props.put("ssl.endpoint.identification.algorithm", broker.getSslEndpointIdentificationAlgorithm());
        }

        return props;
    }

    /**
     * Loads SSL configuration from broker config to UI fields.
     */
    private void loadSslConfigToUI(KafkaBrokerConfig broker) {
        if (broker == null) {
            clearSslFields();
            return;
        }

        boolean hasSsl = broker.getSecurityProtocol() != null &&
                        !broker.getSecurityProtocol().isEmpty() &&
                        !"PLAINTEXT".equals(broker.getSecurityProtocol());

        if (sslEnabledCheckBox != null) {
            sslEnabledCheckBox.setSelected(hasSsl);
        }

        if (sslSecurityProtocolCombo != null) {
            sslSecurityProtocolCombo.setValue(broker.getSecurityProtocol());
        }
        if (sslTruststoreLocationField != null) {
            sslTruststoreLocationField.setText(broker.getSslTruststoreLocation());
        }
        if (sslTruststorePasswordField != null) {
            sslTruststorePasswordField.setText(broker.getSslTruststorePassword());
        }
        if (sslTruststoreTypeCombo != null) {
            sslTruststoreTypeCombo.setValue(broker.getSslTruststoreType() != null ? broker.getSslTruststoreType() : "JKS");
        }
        if (sslKeystoreLocationField != null) {
            sslKeystoreLocationField.setText(broker.getSslKeystoreLocation());
        }
        if (sslKeystorePasswordField != null) {
            sslKeystorePasswordField.setText(broker.getSslKeystorePassword());
        }
        if (sslKeyPasswordField != null) {
            sslKeyPasswordField.setText(broker.getSslKeyPassword());
        }
        if (sslKeystoreTypeCombo != null) {
            sslKeystoreTypeCombo.setValue(broker.getSslKeystoreType() != null ? broker.getSslKeystoreType() : "JKS");
        }
        if (sslEndpointAlgorithmCombo != null) {
            sslEndpointAlgorithmCombo.setValue(broker.getSslEndpointIdentificationAlgorithm() != null ? broker.getSslEndpointIdentificationAlgorithm() : "https");
        }

        updateSslStatusLabel();
    }

    /**
     * Clears all SSL configuration fields.
     */
    private void clearSslFields() {
        if (sslEnabledCheckBox != null) sslEnabledCheckBox.setSelected(false);
        if (sslSecurityProtocolCombo != null) sslSecurityProtocolCombo.setValue("PLAINTEXT");
        if (sslTruststoreLocationField != null) sslTruststoreLocationField.clear();
        if (sslTruststorePasswordField != null) sslTruststorePasswordField.clear();
        if (sslTruststoreTypeCombo != null) sslTruststoreTypeCombo.setValue("JKS");
        if (sslKeystoreLocationField != null) sslKeystoreLocationField.clear();
        if (sslKeystorePasswordField != null) sslKeystorePasswordField.clear();
        if (sslKeyPasswordField != null) sslKeyPasswordField.clear();
        if (sslKeystoreTypeCombo != null) sslKeystoreTypeCombo.setValue("JKS");
        if (sslEndpointAlgorithmCombo != null) sslEndpointAlgorithmCombo.setValue("https");
        if (sslConfigStatusLabel != null) sslConfigStatusLabel.setText("");
    }

    private String getFieldValue(TextField field) {
        if (field == null) return null;
        String value = field.getText().trim();
        return value.isEmpty() ? null : value;
    }

    private String getPasswordValue(PasswordField field) {
        if (field == null) return null;
        String value = field.getText().trim();
        return value.isEmpty() ? null : value;
    }

    private void updateSslStatusLabel() {
        if (sslConfigStatusLabel == null) return;

        if (sslEnabledCheckBox != null && sslEnabledCheckBox.isSelected()) {
            String protocol = sslSecurityProtocolCombo.getValue();
            boolean hasTruststore = sslTruststoreLocationField != null && !sslTruststoreLocationField.getText().trim().isEmpty();
            boolean hasKeystore = sslKeystoreLocationField != null && !sslKeystoreLocationField.getText().trim().isEmpty();

            StringBuilder status = new StringBuilder("SSL Enabled: " + protocol);
            if (hasTruststore) status.append(" | Truststore: OK");
            if (hasKeystore) status.append(" | Keystore: OK (mTLS)");
            sslConfigStatusLabel.setText(status.toString());
        } else {
            sslConfigStatusLabel.setText("SSL/TLS disabled");
        }
    }

    @FXML
    private void handleBrowseTruststore() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Select Truststore File");
        fileChooser.getExtensionFilters().addAll(
            new FileChooser.ExtensionFilter("Keystore Files", "*.jks", "*.p12", "*.pfx"),
            new FileChooser.ExtensionFilter("All Files", "*.*")
        );
        File selectedFile = fileChooser.showOpenDialog(null);
        if (selectedFile != null && sslTruststoreLocationField != null) {
            // Validate file exists
            if (!selectedFile.exists() || !selectedFile.isFile()) {
                showWarningAlert("Invalid File", "The selected file does not exist or is not a valid file.");
                return;
            }
            sslTruststoreLocationField.setText(selectedFile.getAbsolutePath());
            updateSslStatusLabel();
            appendToConsole("[SSL] Truststore selected: " + selectedFile.getAbsolutePath());
        }
    }

    @FXML
    private void handleBrowseKeystore() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Select Keystore File");
        fileChooser.getExtensionFilters().addAll(
            new FileChooser.ExtensionFilter("Keystore Files", "*.jks", "*.p12", "*.pfx"),
            new FileChooser.ExtensionFilter("All Files", "*.*")
        );
        File selectedFile = fileChooser.showOpenDialog(null);
        if (selectedFile != null && sslKeystoreLocationField != null) {
            // Validate file exists
            if (!selectedFile.exists() || !selectedFile.isFile()) {
                showWarningAlert("Invalid File", "The selected file does not exist or is not a valid file.");
                return;
            }
            sslKeystoreLocationField.setText(selectedFile.getAbsolutePath());
            updateSslStatusLabel();
            appendToConsole("[SSL] Keystore selected: " + selectedFile.getAbsolutePath());
        }
    }

    private void filterComboBoxSafely(javafx.scene.control.ComboBox<String> comboBox,
                                      java.util.List<String> allTopics,
                                      String filter,
                                      boolean updatingFlag) {
        String lowerFilter = filter.toLowerCase();
        java.util.List<String> filtered = allTopics.stream()
            .filter(item -> item.toLowerCase().contains(lowerFilter))
            .collect(java.util.stream.Collectors.toList());
        if (!filtered.isEmpty()) {
            boolean isSearch = comboBox == searchTopicComboBox;
            boolean isPush = comboBox == pushTopicComboBox;
            boolean isVerify = comboBox == verifyTopicComboBox;
            boolean isDescribe = comboBox == describeTopicComboBox;
            boolean isUpdate = comboBox == updateTopicComboBox;
            boolean isCgDescribe = comboBox == cgTopicComboBox;
            boolean isCgLag = comboBox == cgLagTopicComboBox;
            boolean isCgReset = comboBox == cgResetTopicComboBox;
            
            if (isSearch) updatingSearchCombo = true;
            else if (isPush) updatingPushCombo = true;
            else if (isVerify) updatingVerifyCombo = true;
            else if (isDescribe) updatingDescribeCombo = true;
            else if (isUpdate) updatingUpdateCombo = true;
            else if (isCgDescribe) updatingCgDescribeCombo = true;
            else if (isCgLag) updatingCgLagCombo = true;
            else if (isCgReset) updatingCgResetCombo = true;
            
            String editorText = comboBox.getEditor().getText();
            int caretPos = comboBox.getEditor().getCaretPosition();
            comboBox.getItems().setAll(filtered);
            comboBox.getEditor().setText(editorText);
            comboBox.getEditor().positionCaret(caretPos);
            if (!comboBox.isShowing() && filtered.size() > 1) {
                comboBox.show();
            }

            if (isSearch) updatingSearchCombo = false;
            else if (isPush) updatingPushCombo = false;
            else if (isVerify) updatingVerifyCombo = false;
            else if (isDescribe) updatingDescribeCombo = false;
            else if (isUpdate) updatingUpdateCombo = false;
            else if (isCgDescribe) updatingCgDescribeCombo = false;
            else if (isCgLag) updatingCgLagCombo = false;
            else if (isCgReset) updatingCgResetCombo = false;
        }
    }

    private void populateKeyFieldComboBox() {
        String template = pushMessageTemplateArea.getText();
        if (template == null || template.trim().isEmpty()) {
            keyFieldComboBox.getItems().clear();
            autoIncrementFieldPicker.getItems().clear();
            rotationFieldPicker.getItems().clear();
            uuidFieldPicker.getItems().clear();
            headerKeyField.getItems().clear();
            headerFieldPicker.getItems().clear();
            return;
        }

        try {
            // Parse JSON to extract field names
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode rootNode = mapper.readTree(template);
            List<String> fieldNames = new ArrayList<>();
            extractFieldNames(rootNode, "", fieldNames);
            keyFieldComboBox.getItems().setAll(fieldNames);
            autoIncrementFieldPicker.getItems().setAll(fieldNames);
            rotationFieldPicker.getItems().setAll(fieldNames);
            uuidFieldPicker.getItems().setAll(fieldNames);
            headerKeyField.getItems().setAll(fieldNames);
            headerFieldPicker.getItems().setAll(fieldNames);
        } catch (Exception e) {
            logger.debug("Could not parse template for field extraction: {}", e.getMessage());
            keyFieldComboBox.getItems().clear();
            autoIncrementFieldPicker.getItems().clear();
            rotationFieldPicker.getItems().clear();
            uuidFieldPicker.getItems().clear();
            headerKeyField.getItems().clear();
            headerFieldPicker.getItems().clear();
        }
    }

    @FXML
    private void handleAddAutoIncrement() {
        String field = autoIncrementFieldPicker.getValue();
        if (field == null || field.isEmpty()) return;
        String start = autoIncrementStartField.getText().trim().isEmpty() ? "1" : autoIncrementStartField.getText().trim();
        String step = autoIncrementStepField.getText().trim().isEmpty() ? "1" : autoIncrementStepField.getText().trim();
        String entry = field + ":" + start + ":" + step;
        String current = autoIncrementFieldsField.getText().trim();
        autoIncrementFieldsField.setText(current.isEmpty() ? entry : current + ", " + entry);
        autoIncrementFieldPicker.setValue(null);
        autoIncrementStartField.clear();
        autoIncrementStepField.clear();
    }

    @FXML
    private void handleClearAutoIncrement() {
        autoIncrementFieldsField.clear();
    }

    @FXML
    private void handleAddRotation() {
        String field = rotationFieldPicker.getValue();
        String values = rotationValuesField.getText().trim();
        if (field == null || field.isEmpty() || values.isEmpty()) return;
        String entry = field + ":" + values;
        String current = rotationFieldsField.getText().trim();
        rotationFieldsField.setText(current.isEmpty() ? entry : current + " ; " + entry);
        rotationFieldPicker.setValue(null);
        rotationValuesField.clear();
    }

    @FXML
    private void handleClearRotation() {
        rotationFieldsField.clear();
    }

    @FXML
    private void handleAddUuid() {
        String field = uuidFieldPicker.getValue();
        if (field == null || field.isEmpty()) return;
        String current = uuidFieldsField.getText().trim();
        uuidFieldsField.setText(current.isEmpty() ? field : current + ", " + field);
        uuidFieldPicker.setValue(null);
    }

    @FXML
    private void handleClearUuid() {
        uuidFieldsField.clear();
    }

    @FXML
    private void handleAddHeader() {
        String key = headerKeyField.getEditor().getText().trim();
        if (key.isEmpty()) return;
        String value = headerFieldPicker.getEditor().getText().trim();
        // If value is empty, store key= (empty) — engine resolves from message field by key name at runtime
        String entry = key + "=" + value;
        String current = messageHeadersField.getText().trim();
        messageHeadersField.setText(current.isEmpty() ? entry : current + ", " + entry);
        headerKeyField.setValue(null);
        headerKeyField.getEditor().clear();
        headerFieldPicker.setValue(null);
        headerFieldPicker.getEditor().clear();
    }

    @FXML
    private void handleClearHeaders() {
        messageHeadersField.clear();
    }

    private void extractFieldNames(com.fasterxml.jackson.databind.JsonNode node, String prefix, List<String> fieldNames) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String fieldName = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                fieldNames.add(fieldName);
                extractFieldNames(entry.getValue(), fieldName, fieldNames);
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                String arrayField = prefix + "[" + i + "]";
                extractFieldNames(node.get(i), arrayField, fieldNames);
            }
        }
    }

    // ========== SEARCH EVENT HANDLERS ==========

    @FXML
    private void handlePeekOne() {
        if (!prepareSearch()) return;
        String topic = searchTopicComboBox.getValue();
        if (!validateSearchInputs(topic)) return;

        Integer partition = parsePartition(true);
        String searchPartVal = searchPartitionField.getValue() != null ? searchPartitionField.getValue() : (searchPartitionField.getEditor().getText() != null ? searchPartitionField.getEditor().getText() : "");
        if (partition == null && searchSpecificPartition.isSelected() && !searchPartVal.trim().isEmpty()) return;

        Long offset = null;

        if (partition != null) {
            String offsetInput = sourceOffsetField.getText();
            if (offsetInput != null && !offsetInput.trim().isEmpty()) {
                try { offset = Long.parseLong(offsetInput.trim()); }
                catch (NumberFormatException e) { searchStatusLabel.setText("\u26a0 Invalid offset"); return; }
            }
        }

        long[] timeRange = parseTimeRange();
        if (timeRange == null) return;

        searchResultsArea.clear();
        searchStatusLabel.setText("Peeking\u2026");
        setSearchInProgress(true);

        final Integer finalPartition = partition;
        final Long finalOffset = offset;
        final Long ff = timeRange[0] >= 0 ? timeRange[0] : null;
        final Long ft = timeRange[1] >= 0 ? timeRange[1] : null;
        final String bootstrapServers = getBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.isEmpty()) return;
        final String peekKeyDeser = sourceKeyDeserializerField.getText();
        final String peekValDeser = searchValueDeserializerField.getText();
        final String peekFilter = buildSearchFilter();
        final String peekTopic = topic.trim();
        searchService.setActiveFlatbufClassName(searchFlatbufClassNameField.getText());
        searchService.setDecodeAsFlatbuf(decodeAsFlatbufCheck.isSelected());
        new Thread(() -> {
            // Auto-download Maven dependencies if configured
            if (!autoDownloadMavenDependencies()) {
                Platform.runLater(() -> {
                    searchStatusLabel.setText("⚠ Failed to download dependencies");
                    setSearchInProgress(false);
                });
                return;
            }
            try {
                KafkaSearchService.PeekResult result = searchService.peekOne(
                        bootstrapServers, peekTopic,
                        peekKeyDeser, peekValDeser,
                        finalPartition, finalOffset, ff, ft, peekFilter,
                        this::appendToConsole);

                // Extract partition/offset from result and store in memory immediately
                Integer peekResultPartition = null;
                Long peekResultOffset = null;
                if (result.isSuccess() && result.getData() != null) {
                    // Parse partition and offset from the JSON result
                    Map<?, ?> parsed = parsePartitionOffsetFromJson(result.getData());
                    if (parsed != null) {
                        peekResultPartition = (Integer) parsed.get("partition");
                        peekResultOffset = (Long) parsed.get("offset");
                    }
                    // Store message template
                    if (result.getMessageJson() != null) {
                        lastSearchMessageTemplate.set(result.getMessageJson());
                    }
                }

                // Store in memory immediately in background thread
                if (peekResultPartition != null && peekResultOffset != null) {
                    lastSearchPartitionOffsets.clear();
                    lastSearchPartitionOffsets.put(peekResultPartition, peekResultOffset);
                    lastSearchTopic = peekTopic;
                    appendToConsole("[Peek] Partition offset captured: partition=" + peekResultPartition + ", offset=" + peekResultOffset);
                }

                final Integer finalPeekPartition = peekResultPartition;
                final Long finalPeekOffset = peekResultOffset;
                Platform.runLater(() -> {
                    if (result.isSuccess()) {
                        ensureSearchResultsVisible();
                        searchResultsArea.setText("=== 1 message found ===\n\n" + result.getData());
                        searchStatusLabel.setText("\u2713 1 message found | " + result.getStatus());
                        if (result.getMessageJson() != null) {
                            pushMessageTemplateArea.setText(result.getMessageJson());
                        }
                        // Also update verify offsets if we got a partition/offset
                        if (finalPeekPartition != null && finalPeekOffset != null) {
                            verifyPartitionOffsetsArea.setText(finalPeekPartition + "=" + (finalPeekOffset + 1));
                            if (verifyTopicComboBox.getValue() == null || verifyTopicComboBox.getValue().isEmpty()) {
                                verifyTopicComboBox.setValue(peekTopic);
                            }
                        }
                    } else {
                        searchStatusLabel.setText("\u2717 " + result.getStatus());
                    }
                });
            } finally {
                setSearchInProgress(false);
            }
        }).start();
    }

    /**
     * Parses partition and offset from JSON result string.
     * Returns map with "partition" (Integer) and "offset" (Long) or null.
     */
    private Map<String, Object> parsePartitionOffsetFromJson(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            Map<?, ?> map = mapper.readValue(json, Map.class);
            Map<String, Object> result = new HashMap<>();
            Object partition = map.get("partition");
            Object offset = map.get("offset");
            if (partition instanceof Number) {
                result.put("partition", ((Number) partition).intValue());
            }
            if (offset instanceof Number) {
                result.put("offset", ((Number) offset).longValue());
            }
            return result.isEmpty() ? null : result;
        } catch (Exception e) {
            return null;
        }
    }

    @FXML
    private void handleSearch() {
        if (!prepareSearch()) return;
        String topic = searchTopicComboBox.getValue();
        if (!validateSearchInputs(topic)) return;

        int maxResults = 100;
        try {
            String maxStr = searchMaxResults.getText();
            if (maxStr != null && !maxStr.trim().isEmpty()) maxResults = Integer.parseInt(maxStr.trim());
        } catch (NumberFormatException e) {
            searchStatusLabel.setText("\u26a0 Invalid max results"); return;
        }

        Integer specificPartition = parsePartition(false);
        if (specificPartition == null && searchSpecificPartition.isSelected()) return;

        Long specificOffset = null;
        if (specificPartition != null) {
            String offsetInput = sourceOffsetField.getText();
            if (offsetInput != null && !offsetInput.trim().isEmpty()) {
                try { specificOffset = Long.parseLong(offsetInput.trim()); }
                catch (NumberFormatException e) { 
                    handleError("Search", "Invalid offset format", e);
                    searchStatusLabel.setText("⚠ Invalid offset"); 
                    return; 
                }
            }
        }

        long[] timeRange = parseTimeRange();
        if (timeRange == null) return;

        searchResultsArea.clear();
        searchStatusLabel.setText("Searching\u2026");
        setSearchInProgress(true);

        final Integer fp = specificPartition;
        final Long fo = specificOffset;
        final Long ff = timeRange[0] >= 0 ? timeRange[0] : null;
        final Long ft = timeRange[1] >= 0 ? timeRange[1] : null;
        final int fm = maxResults;
        final String bootstrapServers = getBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.isEmpty()) return;
        final String keyDeser = sourceKeyDeserializerField.getText();
        final String valDeser = searchValueDeserializerField.getText();
        final String searchFilter = buildSearchFilter();
        final boolean fromBeginning = searchFromBeginningCheck != null && searchFromBeginningCheck.isSelected();
        searchService.setActiveFlatbufClassName(searchFlatbufClassNameField.getText());
        searchService.setDecodeAsFlatbuf(decodeAsFlatbufCheck.isSelected());
        searchStopFlag.set(false);
        new Thread(() -> {
            // Auto-download Maven dependencies if configured
            if (!autoDownloadMavenDependencies()) {
                Platform.runLater(() -> {
                    searchStatusLabel.setText("⚠ Failed to download dependencies");
                    setSearchInProgress(false);
                });
                return;
            }
            try {
                KafkaSearchService.SearchResult result = searchService.searchMessages(
                        bootstrapServers, topic.trim(),
                        keyDeser, valDeser,
                        fp, fo, ff, ft, searchFilter, fm, fromBeginning, this::appendToConsole, searchStopFlag);
                final String resultBroker = bootstrapServers;
                final String resultTopic = topic.trim();
                final String resultTime = java.time.LocalDateTime.now()
                        .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                final List<String> messages = result.isSuccess() ? result.getResults() : null;
                final String resultError = result.isSuccess() ? null : result.getError();
                final int resultCount = messages != null ? messages.size() : 0;
                // Capture partition offsets and first message JSON before releasing result
                final Map<Integer, Long> partitionOffsets = result.isSuccess() ? result.getPartitionOffsets() : null;
                final String firstMessageJson = (messages != null && !messages.isEmpty()) ? messages.get(0) : null;
                final String extractedValue = extractValueFromJson(firstMessageJson);

                // Store in memory IMMEDIATELY in background thread (before UI update)
                if (partitionOffsets != null && !partitionOffsets.isEmpty()) {
                    lastSearchPartitionOffsets.clear();
                    lastSearchPartitionOffsets.putAll(partitionOffsets);
                    lastSearchTopic = resultTopic;
                    appendToConsole("[Search] Partition offsets captured: " + partitionOffsets.size() + " partition(s)");
                }
                if (extractedValue != null) {
                    lastSearchMessageTemplate.set(extractedValue);
                }

                result = null; // release SearchResult + its internal deque immediately; GC before runLater executes
                final String sep = "\n" + "-".repeat(80) + "\n";

                // First runLater: lightweight — template, offsets, clear, header only (no message content)
                Platform.runLater(() -> {
                    if (extractedValue != null) {
                        pushMessageTemplateArea.setText(extractedValue);
                    }
                    if (partitionOffsets != null && !partitionOffsets.isEmpty()) {
                        StringBuilder sb = new StringBuilder();
                        partitionOffsets.entrySet().stream()
                                .sorted(Map.Entry.comparingByKey())
                                .forEach(e -> sb.append(e.getKey()).append("=").append(e.getValue() + 1).append("\n"));
                        verifyPartitionOffsetsArea.setText(sb.toString().trim());
                        if (verifyTopicComboBox.getValue() == null || verifyTopicComboBox.getValue().isEmpty()) {
                            verifyTopicComboBox.setValue(resultTopic);
                        }
                    }
                    ensureSearchResultsVisible();
                    searchResultsArea.clear();
                    String meta = "Bootstrap : " + resultBroker + "\n"
                            + "Pod       : " + getCurrentPodName() + "\n"
                            + "Topic     : " + resultTopic + "\n"
                            + "Searched  : " + resultTime + "\n"
                            + "=".repeat(80) + "\n";
                    if (messages == null) {
                        searchResultsArea.setText("ERROR: " + resultError);
                        searchStatusLabel.setText("Search failed");
                    } else if (messages.isEmpty()) {
                        searchResultsArea.setText(meta + "No matching messages found.");
                        searchStatusLabel.setText("\u2713 0 message(s) found");
                    } else {
                        searchResultsArea.appendText(meta + "=== " + resultCount + " message(s) found ===\n\n");
                        searchStatusLabel.setText("\u23f3 Rendering " + resultCount + " message(s)...");
                    }
                });

                // Kick off async batch rendering — background thread does not block.
                // Each batch schedules the next from within the FX thread so no sleeping needed.
                // setSearchInProgress(false) in finally fires right after this, re-enabling buttons.
                if (messages != null && !messages.isEmpty()) {
                    scheduleNextBatch(messages, 0, resultCount, sep);
                }
            } finally {
                setSearchInProgress(false);
            }
        }).start();
    }

    /**
     * Recursively renders search result batches on the FX thread.
     * Each invocation appends one batch and schedules the next, yielding between batches
     * so the FX thread can render frames and stay responsive.
     */
    private void scheduleNextBatch(List<String> messages, int start, int totalCount, String sep) {
        final int RENDER_BATCH = 25;
        final int end = Math.min(start + RENDER_BATCH, messages.size());
        final boolean isLast = end >= messages.size();
        final int percent = (int) Math.ceil((end * 100.0) / messages.size());
        Platform.runLater(() -> {
            StringBuilder sb = new StringBuilder();
            for (int j = start; j < end; j++) {
                String msg = messages.get(j);
                if (msg != null) {
                    sb.append(msg);
                    if (j < messages.size() - 1) sb.append(sep);
                    messages.set(j, null);
                }
            }
            searchResultsArea.appendText(sb.toString());
            if (isLast) {
                searchStatusLabel.setText("\u2713 " + totalCount + " message(s) found"
                        + (searchStopFlag.get() ? " [stopped]" : ""));
            } else {
                searchStatusLabel.setText("\u23f3 Rendering " + percent + "% ("
                        + end + "/" + totalCount + ")...");
                scheduleNextBatch(messages, end, totalCount, sep);
            }
        });
    }

    /**
     * Extracts the value field from a JSON message string.
     */
    private String extractValueFromJson(String json) {
        if (json == null || json.isEmpty()) return null;
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            Map<?, ?> map = mapper.readValue(json, Map.class);
            Object value = map.get("value");
            if (value instanceof String) {
                return (String) value;
            }
            // If value is a complex object, pretty-print it
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value);
        } catch (Exception e) {
            return json; // Return original if parsing fails
        }
    }

    @FXML
    private void handleStopSearch() {
        searchStopFlag.set(true);
        tailStopFlag.set(true);
        searchStatusLabel.setText("⏹ Stopping...");
        appendToConsole("[Search] Stop requested by user");
    }

    @FXML
    private void handleStartTail() {
        if (tailInProgress || searchInProgress) {
            searchStatusLabel.setText("⏳ Operation in progress, please wait...");
            return;
        }
        String topic = searchTopicComboBox.getValue();
        if (!validateSearchInputs(topic)) return;

        final String bootstrapServers = getBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.isEmpty()) return;

        Integer specificPartition = parsePartition(false);
        if (specificPartition == null && searchSpecificPartition.isSelected()) return;

        final String keyDeser = sourceKeyDeserializerField.getText();
        final String valDeser = searchValueDeserializerField.getText();
        final String searchFilter = buildSearchFilter();
        final Integer fp = specificPartition;

        searchResultsArea.clear();
        ensureSearchResultsVisible();
        searchStatusLabel.setText("⏳ Tailing… 0/200 messages");
        setTailInProgress(true);
        tailStopFlag.set(false);

        new Thread(() -> {
            // Auto-download Maven dependencies if configured
            if (!autoDownloadMavenDependencies()) {
                Platform.runLater(() -> {
                    searchStatusLabel.setText("⚠ Failed to download dependencies");
                    setTailInProgress(false);
                });
                return;
            }
            // Set classloader after auto-download
            tailService.setCustomClassLoader(searchService.getCustomClassLoader());
            tailService.setActiveFlatbufClassName(searchFlatbufClassNameField.getText());
            tailService.setDecodeAsFlatbuf(decodeAsFlatbufCheck.isSelected());

            appendToConsole("[Tail] Starting tail operation on topic: " + topic);
            
            tailService.tail(bootstrapServers, topic.trim(), keyDeser, valDeser, fp, searchFilter,
                    tailStopFlag, new KafkaTailService.TailCallback() {
                @Override
                public void onBatch(List<String> messages, int totalSoFar) {
                    final String sep = "\n" + "-".repeat(80) + "\n";
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < messages.size(); i++) {
                        sb.append(messages.get(i));
                        if (i < messages.size() - 1) sb.append(sep);
                    }
                    final String batch = sb.toString();
                    final int total = totalSoFar;
                    final int batchSize = messages.size();
                    // Limited console logging: log batch arrival and first message preview (first 200 chars)
                    final String firstMsgPreview = messages.isEmpty() ? "" : messages.get(0).substring(0, Math.min(200, messages.get(0).length())).replaceAll("\s+", " ");
                    Platform.runLater(() -> {
                        String current = searchResultsArea.getText();
                        if (current.isEmpty()) {
                            searchResultsArea.setText(batch);
                        } else {
                            searchResultsArea.setText(batch + sep + current);
                        }
                        searchStatusLabel.setText("⏳ Tailing… " + total + "/200 messages");
                        // Log to console (limited - only first message preview)
                        appendToConsole("[Tail] Batch received: " + batchSize + " message(s), total: " + total + " | Preview: " + firstMsgPreview);
                    });
                }

                @Override
                public void onStop(String reason, int total) {
                    Platform.runLater(() -> {
                        searchStatusLabel.setText("✓ Tail stopped: " + reason + " | " + total + " message(s)");
                        appendToConsole("[Tail] Stopped: " + reason + ", " + total + " message(s) total");
                        setTailInProgress(false);
                    });
                }
            }, this::appendToConsole);
        }, "kafka-tail-thread").start();
    }

    private void setTailInProgress(boolean inProgress) {
        this.tailInProgress = inProgress;
        Platform.runLater(() -> {
            if (startTailButton != null) startTailButton.setDisable(inProgress);
            if (searchButton != null)    searchButton.setDisable(inProgress);
            if (peekOneButton != null)   peekOneButton.setDisable(inProgress);
            if (stopSearchButton != null) stopSearchButton.setDisable(!inProgress);
            searchTopicComboBox.setDisable(inProgress);
            searchTextFilter.setDisable(inProgress);
            searchAllPartitions.setDisable(inProgress);
            searchSpecificPartition.setDisable(inProgress);
            searchPartitionField.setDisable(inProgress || !searchSpecificPartition.isSelected());
            sourceKeyDeserializerField.setDisable(inProgress);
            searchValueDeserializerField.setDisable(inProgress);
            searchFlatbufClassNameField.setDisable(inProgress);
            decodeAsFlatbufCheck.setDisable(inProgress);
        });
    }

    @FXML
    private void handleClearSearchResults() {
        searchResultsArea.clear();
        searchStatusLabel.setText("");
    }

    private boolean prepareSearch() {
        if (searchInProgress) {
            searchStatusLabel.setText("\u23f3 Search in progress, please wait...");
            return false;
        }
        ensureSearchResultsVisible();
        return true;
    }

    private void ensureSearchResultsVisible() {
        searchResultsVisible = true;
    }

    private Integer parsePartition(boolean requireOffset) {
        if (!searchSpecificPartition.isSelected()) return null;
        String text = searchPartitionField.getValue() != null ? searchPartitionField.getValue().trim() : "";
        if (text.isEmpty()) {
            text = searchPartitionField.getEditor().getText() != null ? searchPartitionField.getEditor().getText().trim() : "";
        }
        if (text.isEmpty()) return null;
        try { return Integer.parseInt(text); }
        catch (NumberFormatException e) {
            searchStatusLabel.setText("\u26a0 Invalid partition");
            return null;
        }
    }

    private long[] parseTimeRange() {
        if (!searchWithTimeRange.isSelected()) return new long[]{-1, -1};
        try {
            java.time.LocalDate fromDate = searchFromDatePicker.getValue();
            java.time.LocalDate toDate = searchToDatePicker.getValue();
            if (fromDate == null || toDate == null) {
                searchStatusLabel.setText("Select both dates");
                return null;
            }
            long from = LocalDateTime.of(fromDate,
                    java.time.LocalTime.of(searchFromHourSpinner.getValue(), searchFromMinuteSpinner.getValue()))
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            long to = LocalDateTime.of(toDate,
                    java.time.LocalTime.of(searchToHourSpinner.getValue(), searchToMinuteSpinner.getValue(), 59))
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            return new long[]{from, to};
        } catch (Exception e) {
            searchStatusLabel.setText("Time error: " + e.getMessage());
            return null;
        }
    }

    private String buildSearchFilter() {
        return searchTextFilter.getText().trim();
    }

    @FXML
    private void handleCopySearchResults() {
        javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        javafx.scene.input.ClipboardContent content = new javafx.scene.input.ClipboardContent();
        content.putString(searchResultsArea.getText());
        clipboard.setContent(content);
        searchStatusLabel.setText("Copied to clipboard");
    }

    private boolean validateSearchInputs(String topic) {
        if (getSelectedBroker() == null) {
            searchStatusLabel.setText("\u26a0 Please select an active broker");
            return false;
        }
        if (topic == null || topic.trim().isEmpty()) {
            searchStatusLabel.setText("\u26a0 Please enter a topic name");
            return false;
        }
        return true;
    }

    private void setSearchInProgress(boolean inProgress) {
        this.searchInProgress = inProgress;
        Platform.runLater(() -> {
            // Search/Peek action buttons
            if (searchButton != null)    searchButton.setDisable(inProgress);
            if (peekOneButton != null)   peekOneButton.setDisable(inProgress);
            if (startTailButton != null) startTailButton.setDisable(inProgress);
            if (stopSearchButton != null) stopSearchButton.setDisable(!inProgress);

            // All input controls
            searchTopicComboBox.setDisable(inProgress);
            searchTextFilter.setDisable(inProgress);
            searchAllPartitions.setDisable(inProgress);
            searchSpecificPartition.setDisable(inProgress);
            searchPartitionField.setDisable(inProgress || !searchSpecificPartition.isSelected());
            sourceOffsetField.setDisable(inProgress || !searchSpecificPartition.isSelected());
            offsetLabel.setDisable(inProgress || !searchSpecificPartition.isSelected());
            // Time range controls remain enabled during operations (consistent with tail)
            searchMaxResults.setDisable(inProgress);
            searchValueDeserializerField.setDisable(inProgress);
            searchFlatbufClassNameField.setDisable(inProgress);
            decodeAsFlatbufCheck.setDisable(inProgress);
            sourceKeyDeserializerField.setDisable(inProgress);
            configsCombo.setDisable(inProgress);
        });
    }

    private void setPushInProgress(boolean inProgress) {
        Platform.runLater(() -> {
            pushTopicComboBox.setDisable(inProgress);
            partitionKeyAll.setDisable(inProgress);
            partitionKeySpecific.setDisable(inProgress);
            partitionKeyField.setDisable(inProgress);
            partitionKeyConstant.setDisable(inProgress);
            targetPartitionField.setDisable(inProgress || !partitionKeySpecific.isSelected());
            keyFieldComboBox.setDisable(inProgress || !partitionKeyField.isSelected());
            keyConstantField.setDisable(inProgress || !partitionKeyConstant.isSelected());
            boolean flatbufLocked = pushFlatbufModeCheck.isSelected();
            keySerializerField.setDisable(inProgress || flatbufLocked);
            valueSerializerField.setDisable(inProgress || flatbufLocked);
            protobufClassNameField.setDisable(inProgress || flatbufLocked);
            pushFlatbufClassNameField.setDisable(inProgress);
            pushFlatbufModeCheck.setDisable(inProgress);
            messageHeadersField.setDisable(inProgress);
            messageCountField.setDisable(inProgress);
            threadPoolSizeField.setDisable(inProgress);
            batchSizeField.setDisable(inProgress);
            pushMessageTemplateArea.setDisable(inProgress);
            autoIncrementFieldsField.setDisable(inProgress);
            rotationFieldsField.setDisable(inProgress);
            uuidFieldsField.setDisable(inProgress);
            configsCombo.setDisable(inProgress);
        });
    }

    // ========== PUSH EVENT HANDLERS ==========

    @FXML
    private void handleStart() {
        if (engine != null && engine.isRunning()) {
            appendToConsole("[Push] Already running");
            return;
        }

        // Auto-show console when push starts
        if (!consoleVisible) {
            consoleVisible = true;
            pushConsoleBox.setVisible(true);
            pushConsoleBox.setManaged(true);
            pushConsoleBox.setPrefHeight(0.35 * 700);
            pushConsoleBox.setMinHeight(150);
            clearConsoleButton.setVisible(true);
            clearConsoleButton.setManaged(true);
            exportConsoleButton.setVisible(true);
            exportConsoleButton.setManaged(true);
            toggleConsoleButton.setText("\u25bc Logs");
        }
        consoleOutput.clear();
        progressBar.setProgress(0);

        try {
            TestConfiguration config = buildConfiguration();
            appendToConsole("[Push] Starting...");

            String valueSerializer = config.getValueSerializerClass();
            boolean isFlatBuffers = config.isFlatbufMode() || (valueSerializer != null
                    && (valueSerializer.contains(".flatbuf.") || valueSerializer.contains(".flatbuffers.")));
            boolean isProtobuf = !isFlatBuffers && valueSerializer != null
                    && (valueSerializer.contains(".proto.serializer.")
                    || (valueSerializer.endsWith("Serializer")
                    && !valueSerializer.contains("kafka.common.serialization")));

            appendToConsole("Debug: flatbufMode=" + config.isFlatbufMode() + 
                ", isFlatBuffers=" + isFlatBuffers + 
                ", isProtobuf=" + isProtobuf + 
                ", valueSerializer=" + valueSerializer);

            if (isFlatBuffers) {
                appendToConsole("Using FlatBuffers engine");
                engine = createFlatBuffersEngine(config);
            } else if (isProtobuf) {
                appendToConsole("Using Protobuf engine");
                engine = createProtobufEngine(config);
            } else {
                appendToConsole("Using standard JSON engine");
                MessageGenerator<String> messageGenerator = buildMessageGenerator(config);
                engine = new KafkaLoadTestEngine<>(config, messageGenerator, metrics, this::appendToConsole);
            }

            startButton.setDisable(true);
            stopButton.setDisable(false);
            statusLabel.setText("Status: Running");
            progressBar.setProgress(0);
            setPushInProgress(true);

            new Thread(() -> {
                // Auto-download Maven dependencies if configured
                if (!autoDownloadMavenDependencies()) {
                    Platform.runLater(() -> {
                        statusLabel.setText("⚠ Failed to download dependencies");
                        startButton.setDisable(false);
                        stopButton.setDisable(true);
                        setPushInProgress(false);
                    });
                    return;
                }
                try {
                    engine.start();
                } finally {
                    Platform.runLater(() -> {
                        startButton.setDisable(false);
                        stopButton.setDisable(true);
                        statusLabel.setText("Status: Idle");
                        progressBar.setProgress(1.0);
                        setPushInProgress(false);
                        autoFillVerifyTab();
                        appendToConsole("[Push] Completed");
                        if (!selectedConsumerGroupsList.getItems().isEmpty()) {
                            handleStartVerification();
                        }
                    });
                }
            }).start();

            new Thread(this::updateProgress).start();

        } catch (Exception e) {
            handleError("Push", "Error during push operation", e);
            setPushInProgress(false);
            showErrorAlert("Failed to start test", e.getMessage());
        }
    }

    @FXML
    private void handleStop() {
        if (engine != null) {
            engine.stop();
        }
    }

    // ========== CONSOLE & UI TOGGLE ==========

    @FXML
    private void handleClear() {
        consoleOutput.clear();
        progressBar.setProgress(0);
    }

    @FXML
    private void handleToggleConsole() {
        consoleVisible = !consoleVisible;
        pushConsoleBox.setVisible(consoleVisible);
        pushConsoleBox.setManaged(consoleVisible);
        clearConsoleButton.setVisible(consoleVisible);
        clearConsoleButton.setManaged(consoleVisible);
        exportConsoleButton.setVisible(consoleVisible);
        exportConsoleButton.setManaged(consoleVisible);
        if (consoleVisible) {
            pushConsoleBox.setPrefHeight(0.35 * 700); // 35% of default height
            pushConsoleBox.setMinHeight(150);
            consoleOutput.appendText("\n--- Console shown ---\n");
            toggleConsoleButton.setText("\u25bc Logs");
        } else {
            pushConsoleBox.setPrefHeight(0);
            pushConsoleBox.setMinHeight(0);
            toggleConsoleButton.setText("\u25b2 Logs");
        }
    }


    @FXML
    private void handleExportResults() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Export Results");
        fileChooser.setInitialFileName("kafka-results-"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".txt");
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Text Files", "*.txt"));

        File file = fileChooser.showSaveDialog(consoleOutput.getScene().getWindow());
        if (file != null) {
            try {
                java.nio.file.Files.write(file.toPath(), consoleOutput.getText().getBytes());
            } catch (Exception e) {
                appendToConsole("[Export] ERROR: " + e.getMessage());
            }
        }
    }

    @FXML
    private void handleExportSearchResults() {
        FileChooser fileChooser = new FileChooser();
        fileChooser.setTitle("Export Search Results");
        fileChooser.setInitialFileName("search-results-"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".txt");
        fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("Text Files", "*.txt"));

        File file = fileChooser.showSaveDialog(searchResultsArea.getScene().getWindow());
        if (file != null) {
            try {
                java.nio.file.Files.write(file.toPath(), searchResultsArea.getText().getBytes());
                searchStatusLabel.setText("Exported to: " + file.getName());
            } catch (Exception e) {
                searchStatusLabel.setText("ERROR exporting: " + e.getMessage());
            }
        }
    }

    @FXML
    private void handleClose() {
        // Confirmation dialog
        javafx.scene.control.Alert confirm = new javafx.scene.control.Alert(
            javafx.scene.control.Alert.AlertType.CONFIRMATION,
            "Are you sure you want to close the application?\n\nThis will:\n" +
            "- Disconnect all connected pods\n" +
            "- Delete cache folder data (consumer group cache only)",
            javafx.scene.control.ButtonType.YES,
            javafx.scene.control.ButtonType.NO
        );
        confirm.setTitle("Close Application");
        confirm.setHeaderText(null);

        confirm.showAndWait().ifPresent(response -> {
            if (response == javafx.scene.control.ButtonType.YES) {
                performApplicationShutdown();
            }
        });
    }

    /**
     * Performs complete application shutdown:
     * - Closes all AdminClient connections
     * - Deletes all consumer group cache files
     * - Exits the application
     */
    private void performApplicationShutdown() {
        appendToConsole("[Close] Shutting down application...");

        // Close all AdminClient connections
        for (Map.Entry<String, org.apache.kafka.clients.admin.AdminClient> entry : podAdminClients.entrySet()) {
            try {
                entry.getValue().close();
                logger.info("[Close] Closed AdminClient for pod: {}", entry.getKey());
            } catch (Exception e) {
                logger.warn("[Close] Error closing AdminClient for {}: {}", entry.getKey(), e.getMessage());
            }
        }
        podAdminClients.clear();
        connectedPods.clear();

        // Delete all consumer group cache files
        deleteAllCgCacheFiles();

        // Stop console flush scheduler
        stopConsoleFlushScheduler();

        appendToConsole("[Close] Application closed");

        // Exit the application
        javafx.application.Platform.exit();
        System.exit(0);
    }

    @FXML
    private void handleConnect() {
        // Check if the dropdown-selected pod is already connected
        if (dropdownSelectedBroker != null && isPodConnected(dropdownSelectedBroker.getName())) {
            disconnectPod(dropdownSelectedBroker.getName());
        } else {
            connectPod();
        }
    }

    /**
     * Checks if a specific pod is connected.
     */
    private boolean isPodConnected(String podName) {
        return connectedPods.contains(podName);
    }

    /**
     * Gets the AdminClient for the currently selected pod.
     */
    private org.apache.kafka.clients.admin.AdminClient getCurrentAdminClient() {
        if (currentPodName == null) return null;
        return podAdminClients.get(currentPodName);
    }

    private void connectPod() {
        // Get the broker from the dropdown selection
        KafkaBrokerConfig brokerToConnect = selectedForWorkComboBox.getValue();
        if (brokerToConnect == null) {
            appendToConsole("[Connect] ERROR: Please select a broker from the dropdown first");
            return;
        }

        String brokers = brokerToConnect.getBootstrapServers();
        if (brokers == null || brokers.isEmpty()) {
            appendToConsole("[Connect] ERROR: Selected broker has empty servers");
            return;
        }

        String podName = brokerToConnect.getName();

        // Check if already connected to this pod
        if (isPodConnected(podName)) {
            appendToConsole(podName, "[Connect] Already connected to " + podName);
            return;
        }

        appendToConsole(podName, "[Connect] Starting connection to " + podName + "...");

        // Disable button and show connecting state
        Platform.runLater(() -> {
            connectButton.setDisable(true);
            connectButton.setText("Connecting...");
            connectButton.setStyle("-fx-background-color: #f39c12; -fx-text-fill: white; -fx-font-size: 10px; -fx-font-weight: bold;");
        });

        // Run connection in background thread
        new Thread(() -> {
            org.apache.kafka.clients.admin.AdminClient adminClient = null;
            try {
                java.util.Properties props = new java.util.Properties();
                props.put("bootstrap.servers", brokers);
                props.put("request.timeout.ms", "600000");
                props.put("connections.max.idle.ms", "10000");

                // Apply SSL configuration if enabled
                if (brokerToConnect.getSecurityProtocol() != null && !brokerToConnect.getSecurityProtocol().isEmpty()) {
                    props.put("security.protocol", brokerToConnect.getSecurityProtocol());
                    appendToConsole(podName, "[Connect] Using security protocol: " + brokerToConnect.getSecurityProtocol());

                    // Truststore configuration (required for SSL/SASL_SSL)
                    if (brokerToConnect.getSslTruststoreLocation() != null && !brokerToConnect.getSslTruststoreLocation().isEmpty()) {
                        props.put("ssl.truststore.location", brokerToConnect.getSslTruststoreLocation());
                        if (brokerToConnect.getSslTruststorePassword() != null) {
                            props.put("ssl.truststore.password", brokerToConnect.getSslTruststorePassword());
                        }
                        if (brokerToConnect.getSslTruststoreType() != null) {
                            props.put("ssl.truststore.type", brokerToConnect.getSslTruststoreType());
                        }
                        appendToConsole(podName, "[Connect] Truststore: " + brokerToConnect.getSslTruststoreLocation());
                    }

                    // Keystore configuration (for mutual TLS)
                    if (brokerToConnect.getSslKeystoreLocation() != null && !brokerToConnect.getSslKeystoreLocation().isEmpty()) {
                        props.put("ssl.keystore.location", brokerToConnect.getSslKeystoreLocation());
                        if (brokerToConnect.getSslKeystorePassword() != null) {
                            props.put("ssl.keystore.password", brokerToConnect.getSslKeystorePassword());
                        }
                        if (brokerToConnect.getSslKeyPassword() != null) {
                            props.put("ssl.key.password", brokerToConnect.getSslKeyPassword());
                        }
                        if (brokerToConnect.getSslKeystoreType() != null) {
                            props.put("ssl.keystore.type", brokerToConnect.getSslKeystoreType());
                        }
                        appendToConsole(podName, "[Connect] Keystore configured (mTLS enabled)");
                    }

                    // Endpoint identification algorithm
                    if (brokerToConnect.getSslEndpointIdentificationAlgorithm() != null) {
                        props.put("ssl.endpoint.identification.algorithm", brokerToConnect.getSslEndpointIdentificationAlgorithm());
                    }
                }

                adminClient = org.apache.kafka.clients.admin.AdminClient.create(props);

                // List topics with increased timeout
                java.util.Set<String> topics = adminClient.listTopics().names().get(600, java.util.concurrent.TimeUnit.SECONDS);

                java.util.List<String> sortedTopics = new java.util.ArrayList<>(topics);
                java.util.Collections.sort(sortedTopics);

                // Store the client, topics, and mark as connected
                final org.apache.kafka.clients.admin.AdminClient finalClient = adminClient;
                final List<String> finalTopics = new ArrayList<>(sortedTopics);

                Platform.runLater(() -> {
                    podAdminClients.put(podName, finalClient);
                    connectedPods.add(podName);
                    podTopics.put(podName, finalTopics);  // Store topics for this pod

                    // Always update topics when connecting - this is a fresh connection
                    // Make this pod the active one and show its topics
                    setActivePod(brokerToConnect);
                    updateTopicsForPod(podName, finalTopics);

                    updateActivePodsDisplay();
                    updateConnectButtonState();
                    connectButton.setDisable(false);
                });

                appendToConsole(podName, "[Connect] " + podName + " connected. Topics: " + topics.size());

                // Consumer groups are NOT fetched on connect - they are lazy-loaded from file or fetched on demand
                // This prevents memory spikes and unnecessary API calls
                appendToConsole(podName, "[Connect] Consumer groups will be loaded on-demand from file or Kafka");

            } catch (Exception e) {
                Platform.runLater(() -> {
                    updateConnectButtonState();
                    connectButton.setDisable(false);
                });
                appendToConsole(podName, "[Connect] ERROR connecting to " + podName + ": " + e.getMessage());
                logger.error("[Connect] Error", e);
                if (adminClient != null) {
                    adminClient.close();
                }
            }
        }).start();
    }

    /**
     * Fetches and caches consumer group information for a pod.
     * Called during connect in background thread.
     * Uses parallel processing with 25 threads for optimal performance.
     */
    private void fetchAndCacheConsumerGroups(String podName,
                                             org.apache.kafka.clients.admin.AdminClient adminClient,
                                             List<String> topics) {
        try {
            appendToConsole("[Consumer Groups] Fetching for " + podName + " with " + CONSUMER_GROUP_THREAD_POOL + " threads...");

            Map<String, List<ConsumerGroupInfo>> topicToGroupsMap = fetchAllConsumerGroupsForPod(adminClient, topics);

            // Write to file — no in-memory map retained
            writeCgCacheToFile(podName, topicToGroupsMap);
            topicToGroupsMap.clear(); // release memory immediately after writing

            int groupsFound = cgCacheFile(podName).exists() ? -1 : 0; // just confirm file written
            appendToConsole("[Consumer Groups] Cache written to file for " + podName);

        } catch (Exception e) {
            logger.warn("[Consumer Groups] Error fetching for {}: {}", podName, e.getMessage());
        }
    }

    /**
     * Fetches all consumer groups for a pod and writes them to the cache file.
     * This is called on-demand when file cache is missing or expired.
     * Returns the fetched data (which is also written to file).
     */
    private Map<String, List<ConsumerGroupInfo>> fetchAllConsumerGroupsForPodAndCacheToFile(
            String podName,
            org.apache.kafka.clients.admin.AdminClient adminClient) throws Exception {

        if (!podsFetchingConsumerGroups.add(podName)) {
            // Another thread is already fetching for this pod - wait for it to complete
            logger.info("[CG File Storage] Another thread is already fetching for {}, waiting...", podName);
            int retries = 0;
            while (podsFetchingConsumerGroups.contains(podName) && retries < 30) {
                Thread.sleep(1000);
                retries++;
                // Check if file was created by the other thread
                java.io.File file = cgCacheFile(podName);
                if (file.exists()) {
                    List<ConsumerGroupInfo> testRead = readGroupsForTopicFromFile(podName, "__test__");
                    if (testRead != null) {
                        podsFetchingConsumerGroups.remove(podName);
                        // Read all data from file and return
                        return readAllGroupsFromFile(podName);
                    }
                }
            }
        }

        try {
            appendToConsole("[CG File Storage] Fetching all consumer groups for " + podName + "...");
            Map<String, List<ConsumerGroupInfo>> topicToGroupsMap = fetchAllConsumerGroupsForPod(adminClient, null);

            // Write to file
            writeCgCacheToFile(podName, topicToGroupsMap);
            appendToConsole("[CG File Storage] Saved " + topicToGroupsMap.size() + " topics to file for " + podName);

            return topicToGroupsMap;
        } finally {
            podsFetchingConsumerGroups.remove(podName);
        }
    }

    /**
     * Reads all consumer groups from the cache file for a pod.
     */
    @SuppressWarnings("unchecked")
    private Map<String, List<ConsumerGroupInfo>> readAllGroupsFromFile(String podName) {
        java.io.File file = cgCacheFile(podName);
        if (!file.exists()) return new HashMap<>();

        Map<String, List<ConsumerGroupInfo>> result = new HashMap<>();
        try {
            com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = om.readTree(file);

            // Check timestamp
            com.fasterxml.jackson.databind.JsonNode tsNode = root.get("_timestamp");
            if (tsNode != null && (System.currentTimeMillis() - tsNode.asLong()) > CONSUMER_GROUP_FILE_STORAGE_TTL_MS) {
                logger.info("[CG File Storage] Expired for pod {}", podName);
                return new HashMap<>(); // expired
            }

            for (java.util.Iterator<String> it = root.fieldNames(); it.hasNext(); ) {
                String key = it.next();
                if ("_timestamp".equals(key)) continue;
                com.fasterxml.jackson.databind.JsonNode topicNode = root.get(key);
                if (topicNode != null && topicNode.isArray()) {
                    List<ConsumerGroupInfo> groups = new ArrayList<>();
                    for (com.fasterxml.jackson.databind.JsonNode gNode : topicNode) {
                        ConsumerGroupInfo cg = new ConsumerGroupInfo(
                            gNode.path("groupId").asText(), gNode.path("topic").asText());
                        com.fasterxml.jackson.databind.JsonNode mems = gNode.get("members");
                        if (mems != null && mems.isArray()) {
                            for (com.fasterxml.jackson.databind.JsonNode mn : mems) {
                                ConsumerMemberInfo mi = new ConsumerMemberInfo(
                                    mn.path("memberId").asText(),
                                    mn.path("clientId").asText(),
                                    mn.path("host").asText());
                                com.fasterxml.jackson.databind.JsonNode parts = mn.get("partitions");
                                if (parts != null && parts.isArray())
                                    parts.forEach(p -> mi.assignedPartitions.add(p.asText()));
                                cg.members.add(mi);
                            }
                        }
                        groups.add(cg);
                    }
                    result.put(key, groups);
                }
            }
        } catch (Exception e) {
            logger.warn("[CG File Storage] Failed to read all groups from file for {}: {} - deleting corrupted file", podName, e.getMessage());
            // Delete corrupted file so it will be re-fetched from Kafka
            file.delete();
        }
        return result;
    }

    // ========== FILE-BASED CONSUMER GROUP STORAGE HELPERS ==========

    private java.io.File cgCacheFile(String podName) {
        String safeName = podName.replaceAll("[^a-zA-Z0-9._-]", "_");
        // Store in kafka-pilot/cache directory for cross-platform compatibility
        java.io.File cacheDir = new java.io.File("kafka-pilot", "cache");
        if (!cacheDir.exists()) {
            cacheDir.mkdirs();
        }
        return new java.io.File(cacheDir, "cg-cache-" + safeName + ".json");
    }

    /**
     * Deletes all consumer group cache files in kafka-pilot/cache directory.
     * Called when application closes.
     */
    private void deleteAllCgCacheFiles() {
        java.io.File cacheDir = new java.io.File("kafka-pilot", "cache");
        if (cacheDir.exists()) {
            java.io.File[] files = cacheDir.listFiles((dir, name) -> name.startsWith("cg-cache-") && name.endsWith(".json"));
            if (files != null) {
                for (java.io.File file : files) {
                    if (file.delete()) {
                        logger.info("[CG File Storage] Deleted on close: {}", file.getName());
                    }
                }
            }
        }
    }

    private void writeCgCacheToFile(String podName, Map<String, List<ConsumerGroupInfo>> data) {
        java.io.File file = cgCacheFile(podName);
        java.io.File tempFile = new java.io.File(file.getParentFile(), file.getName() + ".tmp");
        try {
            com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
            // Build a plain serializable structure: Map<topic, List<Map>>
            Map<String, Object> root = new java.util.LinkedHashMap<>();
            root.put("_timestamp", System.currentTimeMillis());
            for (Map.Entry<String, List<ConsumerGroupInfo>> e : data.entrySet()) {
                List<Map<String, Object>> groups = new ArrayList<>();
                for (ConsumerGroupInfo cg : e.getValue()) {
                    Map<String, Object> g = new java.util.LinkedHashMap<>();
                    g.put("groupId", cg.groupId);
                    g.put("topic", cg.topic);
                    List<Map<String, Object>> members = new ArrayList<>();
                    for (ConsumerMemberInfo m : cg.members) {
                        Map<String, Object> mb = new java.util.LinkedHashMap<>();
                        mb.put("memberId", m.memberId);
                        mb.put("clientId", m.clientId);
                        mb.put("host", m.host);
                        mb.put("partitions", m.assignedPartitions);
                        members.add(mb);
                    }
                    g.put("members", members);
                    groups.add(g);
                }
                root.put(e.getKey(), groups);
            }
            // Atomic write: write to temp file first, then rename
            om.writeValue(tempFile, root);
            if (file.exists()) {
                file.delete();
            }
            tempFile.renameTo(file);
            logger.info("[CG File Storage] Written {} topics to {}", data.size(), file.getName());
        } catch (Exception e) {
            logger.warn("[CG File Storage] Failed to write cache for {}: {}", podName, e.getMessage());
            tempFile.delete();
        }
    }

    /**
     * Reads only the consumer group list for a single topic from the pod's cache file.
     * This avoids loading the entire file into memory.
     */
    @SuppressWarnings("unchecked")
    private List<ConsumerGroupInfo> readGroupsForTopicFromFile(String podName, String topic) {
        java.io.File file = cgCacheFile(podName);
        if (!file.exists()) return null;
        // Check TTL via file last-modified (fallback) or _timestamp field
        try {
            com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = om.readTree(file);
            // Check timestamp
            com.fasterxml.jackson.databind.JsonNode tsNode = root.get("_timestamp");
            if (tsNode != null && (System.currentTimeMillis() - tsNode.asLong()) > CONSUMER_GROUP_FILE_STORAGE_TTL_MS) {
                logger.info("[CG File Storage] Expired for pod {}", podName);
                return null; // expired
            }
            com.fasterxml.jackson.databind.JsonNode topicNode = root.get(topic);
            if (topicNode == null || !topicNode.isArray()) return Collections.emptyList();
            List<ConsumerGroupInfo> result = new ArrayList<>();
            for (com.fasterxml.jackson.databind.JsonNode gNode : topicNode) {
                ConsumerGroupInfo cg = new ConsumerGroupInfo(
                    gNode.path("groupId").asText(), gNode.path("topic").asText());
                com.fasterxml.jackson.databind.JsonNode mems = gNode.get("members");
                if (mems != null && mems.isArray()) {
                    for (com.fasterxml.jackson.databind.JsonNode mn : mems) {
                        ConsumerMemberInfo mi = new ConsumerMemberInfo(
                            mn.path("memberId").asText(),
                            mn.path("clientId").asText(),
                            mn.path("host").asText());
                        com.fasterxml.jackson.databind.JsonNode parts = mn.get("partitions");
                        if (parts != null && parts.isArray())
                            parts.forEach(p -> mi.assignedPartitions.add(p.asText()));
                        cg.members.add(mi);
                    }
                }
                result.add(cg);
            }
            return result;
        } catch (Exception e) {
            logger.warn("[CG File Storage] Failed to read topic {} from file for {}: {} - deleting corrupted file", topic, podName, e.getMessage());
            // Delete corrupted file so it will be re-fetched from Kafka
            file.delete();
            return null;
        }
    }

    /** Returns true if the cache file for this pod exists and is not expired. */
    private boolean isCgCacheValid(String podName) {
        java.io.File file = cgCacheFile(podName);
        if (!file.exists()) return false;
        try {
            com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
            com.fasterxml.jackson.databind.JsonNode root = om.readTree(file);
            com.fasterxml.jackson.databind.JsonNode tsNode = root.get("_timestamp");
            return tsNode != null && (System.currentTimeMillis() - tsNode.asLong()) <= CONSUMER_GROUP_FILE_STORAGE_TTL_MS;
        } catch (Exception e) {
            // Delete corrupted file
            file.delete();
            return false;
        }
    }

    private void deleteCgCacheFile(String podName) {
        java.io.File file = cgCacheFile(podName);
        if (file.exists() && file.delete()) {
            logger.info("[CG File Storage] Deleted cache file for pod {}", podName);
        }
    }

    /**
     * Fetches consumer lag for a specific ConsumerGroupInfo.
     * This is called at runtime when displaying lag for a selected group.
     */
    private void fetchConsumerLagForInfo(ConsumerGroupInfo info,
                                          org.apache.kafka.clients.admin.AdminClient client) throws Exception {
        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
            client.listConsumerGroupOffsets(info.groupId)
                .partitionsToOffsetAndMetadata().get(20, java.util.concurrent.TimeUnit.SECONDS);

        java.util.Set<org.apache.kafka.common.TopicPartition> tps = offsets.keySet().stream()
            .filter(tp -> tp.topic().equals(info.topic))
            .collect(java.util.stream.Collectors.toSet());

        if (tps.isEmpty()) return;

        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
            client.listOffsets(
                tps.stream().collect(java.util.stream.Collectors.toMap(
                    tp -> tp,
                    tp -> org.apache.kafka.clients.admin.OffsetSpec.forTimestamp(java.lang.Long.MAX_VALUE)
                ))
            ).all().get(20, java.util.concurrent.TimeUnit.SECONDS);

        long totalLag = 0;
        for (org.apache.kafka.common.TopicPartition tp : tps) {
            long committed = offsets.get(tp).offset();
            long end = endOffsets.get(tp).offset();
            long lag = Math.max(0, end - committed);
            info.partitionLag.put(tp.partition() + "", lag);
            totalLag += lag;
        }
        info.totalLag = totalLag;
    }

    /**
     * Updates topics for the specified pod and stores them in cache.
     */
    private void updateTopicsForPod(String podName, List<String> topics) {
        podTopics.put(podName, new ArrayList<>(topics));  // Cache topics
        allSearchTopics.clear();
        allSearchTopics.addAll(topics);
        allPushTopics.clear();
        allPushTopics.addAll(topics);
        allVerifyTopics.clear();
        allVerifyTopics.addAll(topics);
        searchTopicComboBox.getItems().setAll(topics);
        pushTopicComboBox.getItems().setAll(topics);
        verifyTopicComboBox.getItems().setAll(topics);
        // Also refresh Topics tab dropdowns
        refreshTopicsTabDropdowns();
    }

    /**
     * Disconnects a specific pod.
     */
    private void disconnectPod(String podName) {
        org.apache.kafka.clients.admin.AdminClient client = podAdminClients.remove(podName);
        if (client != null) {
            client.close();
        }
        connectedPods.remove(podName);
        podTopics.remove(podName);  // Clear cached topics for this pod
        deleteCgCacheFile(podName); // Delete the on-disk consumer group cache
        podStateMap.remove(podName); // Remove saved state for disconnected pod

        // If we disconnected the currently active pod, switch to another or clear
        if (podName.equals(currentPodName)) {
            if (!connectedPods.isEmpty()) {
                // Switch to first available connected pod
                String newPod = connectedPods.iterator().next();
                brokerConfigs.stream()
                    .filter(b -> b.getName().equals(newPod))
                    .findFirst()
                    .ifPresent(this::setActivePod);
            } else {
                setActivePod(null);
                // Clear topics
                Platform.runLater(() -> {
                    searchTopicComboBox.getItems().clear();
                    pushTopicComboBox.getItems().clear();
                    verifyTopicComboBox.getItems().clear();
                });
            }
        }

        Platform.runLater(() -> {
            updateActivePodsDisplay();
            updateConnectButtonState();
        });

        appendToConsole("[Connect] Disconnected from " + podName);
    }

    /**
     * Updates the Active Pod(s) field with comma-separated connected pod names.
     */
    private void updateActivePodsDisplay() {
        if (connectedPods.isEmpty()) {
            activePodsField.setText("");
        } else {
            String podsList = String.join(", ", connectedPods);
            activePodsField.setText(podsList);
        }
    }

    private void fetchPartitionsForTopic(String topic, ComboBox<String> partitionCombo) {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) return;
        new Thread(() -> {
            try {
                java.util.Map<String, org.apache.kafka.clients.admin.TopicDescription> descriptions =
                        client.describeTopics(org.apache.kafka.common.TopicCollection.ofTopicNames(Collections.singletonList(topic)))
                                .allTopicNames().get(10, java.util.concurrent.TimeUnit.SECONDS);
                org.apache.kafka.clients.admin.TopicDescription desc = descriptions.get(topic);
                if (desc != null) {
                    List<String> partitionNumbers = new ArrayList<>();
                    for (int i = 0; i < desc.partitions().size(); i++) {
                        partitionNumbers.add(String.valueOf(i));
                    }
                    Platform.runLater(() -> {
                        String current = partitionCombo.getValue();
                        partitionCombo.getItems().setAll(partitionNumbers);
                        if (current != null) partitionCombo.setValue(current);
                    });
                }
            } catch (Exception e) {
                // Silent fail
            }
        }).start();
    }

    // ========== MAVEN DEPENDENCY ==========

    @FXML
    private void handleDownloadDependency() {
        String dependency = mavenDependencyField.getText();
        if (dependency == null || dependency.trim().isEmpty()) {
            return;
        }

        appendToConsole("[Download] Starting...");
        new Thread(() -> {
            try {
                String repoUrl = mavenRepoUrlField.getText();
                if (repoUrl != null && !repoUrl.trim().isEmpty()) {
                    mavenResolver.setCustomRepository(repoUrl.trim());
                    appendToConsole("[Download] Using repo: " + repoUrl.trim());
                }
                // Support comma-separated dependencies
                List<MavenDependencyResolver.MavenDependency> deps = parseMavenDependencies(dependency);
                appendToConsole("[Download] Resolving " + deps.size() + " dependencies...");

                List<File> allFiles = new ArrayList<>();
                for (MavenDependencyResolver.MavenDependency dep : deps) {
                    appendToConsole("[Download] Resolving: " + dep);
                    List<File> files = mavenResolver.resolveDependency(
                            dep.getGroupId(), dep.getArtifactId(), dep.getVersion());
                    allFiles.addAll(files);
                }

                // Load all dependencies into a single ClassLoader
                URLClassLoader loader = mavenResolver.loadDependenciesIntoClassLoader(allFiles);
                searchService.setCustomClassLoader(loader);

                appendToConsole("[Download] Completed: " + allFiles.size() + " total artifacts");
            } catch (Exception e) {
                logger.error("[Download] Error", e);
                Platform.runLater(() -> {
                    appendToConsole("[Download] ERROR: " + e.getMessage());
                    showErrorAlert("Download Failed", e.getMessage());
                });
            }
        }).start();
    }

    @FXML
    private void handleOpenMavenRepo() {
        String dependency = mavenDependencyField.getText();
        String customRepoUrl = mavenRepoUrlField.getText();
        String url;

        // If custom Maven Repo URL is provided, use it
        if (customRepoUrl != null && !customRepoUrl.trim().isEmpty()) {
            String repoUrl = customRepoUrl.trim();
            if (dependency != null && !dependency.trim().isEmpty()) {
                String first = dependency.split(",")[0].trim();
                String[] parts = first.split(":");
                if (parts.length >= 2) {
                    // Construct artifact URL in standard Maven repo format
                    String groupPath = parts[0].trim().replace(".", "/");
                    String artifactId = parts[1].trim();
                    String version = parts.length >= 3 ? parts[2].trim() : "";
                    if (!version.isEmpty()) {
                        url = repoUrl + (repoUrl.endsWith("/") ? "" : "/") + groupPath + "/" + artifactId + "/" + version + "/";
                    } else {
                        url = repoUrl + (repoUrl.endsWith("/") ? "" : "/") + groupPath + "/" + artifactId + "/";
                    }
                } else {
                    url = repoUrl;
                }
            } else {
                url = repoUrl;
            }
            appendToConsole("[Maven Repo] Opening custom repository: " + url);
        } else {
            // Use default mvnrepository.com
            if (dependency != null && !dependency.trim().isEmpty()) {
                String first = dependency.split(",")[0].trim();
                String[] parts = first.split(":");
                if (parts.length >= 2) {
                    url = "https://mvnrepository.com/artifact/" + parts[0].trim() + "/" + parts[1].trim()
                            + (parts.length >= 3 ? "/" + parts[2].trim() : "");
                } else {
                    url = "https://mvnrepository.com/search?q=" + first.replace(" ", "+");
                }
            } else {
                url = "https://mvnrepository.com/";
            }
            appendToConsole("[Maven Repo] Opening: " + url);
        }

        try {
            Desktop.getDesktop().browse(new URI(url));
        } catch (Exception e) {
            logger.error("Failed to open Maven repo URL", e);
            Platform.runLater(() -> showErrorAlert("Browser Error", "Could not open: " + url));
        }
    }

    /**
     * Parses comma-separated Maven dependencies.
     * Example: "com.google.guava:guava:31.1-jre,org.apache.commons:commons-lang3:3.12.0"
     */
    private List<MavenDependencyResolver.MavenDependency> parseMavenDependencies(String input) {
        List<MavenDependencyResolver.MavenDependency> result = new ArrayList<>();
        String[] parts = input.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(MavenDependencyResolver.MavenDependency.parse(trimmed));
            }
        }
        return result;
    }

    /**
     * Auto-downloads Maven dependencies if configured and not already loaded.
     * Called before search, peek, tail, and push operations.
     * @return true if dependencies are ready (or not needed), false if download failed
     */
    private boolean autoDownloadMavenDependencies() {
        String dependency = mavenDependencyField.getText();
        if (dependency == null || dependency.trim().isEmpty()) {
            return true; // No dependencies configured, proceed without classloader
        }

        // Check if already loaded
        if (searchService.getCustomClassLoader() != null) {
            return true; // Already have a classloader
        }

        appendToConsole("[Auto-Download] Resolving Maven dependencies...");
        try {
            String repoUrl = mavenRepoUrlField.getText();
            if (repoUrl != null && !repoUrl.trim().isEmpty()) {
                mavenResolver.setCustomRepository(repoUrl.trim());
                appendToConsole("[Auto-Download] Using repo: " + repoUrl.trim());
            }

            List<MavenDependencyResolver.MavenDependency> deps = parseMavenDependencies(dependency);
            appendToConsole("[Auto-Download] Resolving " + deps.size() + " dependencies...");

            List<File> allFiles = new ArrayList<>();
            for (MavenDependencyResolver.MavenDependency dep : deps) {
                appendToConsole("[Auto-Download] Resolving: " + dep);
                List<File> files = mavenResolver.resolveDependency(
                        dep.getGroupId(), dep.getArtifactId(), dep.getVersion());
                allFiles.addAll(files);
            }

            URLClassLoader loader = mavenResolver.loadDependenciesIntoClassLoader(allFiles);
            searchService.setCustomClassLoader(loader);
            tailService.setCustomClassLoader(loader);

            appendToConsole("[Auto-Download] Completed: " + allFiles.size() + " artifacts loaded");
            return true;
        } catch (Exception e) {
            logger.error("[Auto-Download] Error", e);
            appendToConsole("[Auto-Download] ERROR: " + e.getMessage());
            return false;
        }
    }

    // ========== CONFIGURATION ==========

    @FXML private void handleSaveConfig()    { saveConfig(configsCombo); }
    @FXML private void handleLoadConfig()    { loadConfig(configsCombo); }
    @FXML private void handleDeleteConfig()  { deleteConfig(configsCombo); }

    private void saveConfig(ComboBox<String> combo) {
        TextInputDialog dialog = new TextInputDialog("my-config");
        dialog.setTitle("Save Configuration");
        dialog.setHeaderText(null);
        dialog.setContentText("Config name:");
        dialog.showAndWait().ifPresent(name -> {
            try {
                TestConfiguration config = buildFullConfiguration();
                config.setConfigName(name);
                configManager.saveConfiguration(config, name);
                appendToConsole("[Config] Saved: " + name);
                refreshCombo(combo);
                combo.setValue(name); // Auto-select the saved config
            } catch (Exception e) {
                handleError("Config", "Error saving configuration", e);
            }
        });
    }

    private void loadConfig(ComboBox<String> combo) {
        String selected = combo.getValue();
        if (selected == null || selected.isEmpty()) {
            return;
        }
        try {
            // Flush current pod's live state before replacing podStateMap
            if (currentPodName != null && !currentPodName.isEmpty()) {
                saveCurrentPodState(currentPodName);
            }
            TestConfiguration config = configManager.loadConfiguration(selected);
            applyFullConfiguration(config);
            appendToConsole("[Config] Loaded: " + selected);
        } catch (Exception e) {
            handleError("Config", "Error loading configuration", e);
        }
    }

    private void deleteConfig(ComboBox<String> combo) {
        String selected = combo.getValue();
        if (selected == null || selected.isEmpty()) {
            return;
        }

        // Confirmation dialog
        javafx.scene.control.Alert confirm = new javafx.scene.control.Alert(
            javafx.scene.control.Alert.AlertType.CONFIRMATION,
            "Are you sure you want to delete configuration '" + selected + "'?",
            javafx.scene.control.ButtonType.YES,
            javafx.scene.control.ButtonType.NO
        );
        confirm.setTitle("Delete Configuration");
        confirm.setHeaderText(null);

        confirm.showAndWait().ifPresent(response -> {
            if (response == javafx.scene.control.ButtonType.YES) {
                try {
                    configManager.deleteConfiguration(selected);
                    appendToConsole("[Config] Deleted: " + selected);
                    refreshCombo(combo);
                } catch (Exception e) {
                    handleError("Config", "Error deleting configuration", e);
                }
            }
        });
    }

    private void refreshCombo(ComboBox<String> combo) {
        List<String> configs = configManager.listConfigurations();
        combo.getItems().clear();
        combo.getItems().addAll(configs);
    }

    private void refreshAllConfigs() {
        refreshCombo(configsCombo);
    }

    // ========== CONFIGURATION BUILDING ==========

    private TestConfiguration buildFullConfiguration() {
        TestConfiguration config = new TestConfiguration();

        // Global settings
        KafkaBrokerConfig selectedBroker = getSelectedBroker();
        if (selectedBroker != null) {
            config.setSelectedBrokerName(selectedBroker.getName());
            config.setBootstrapServers(selectedBroker.getBootstrapServers());
        }
        // Maven dependency is global (common for all pods)
        config.setMavenDependency(mavenDependencyField.getText());
        config.setMavenRepoUrl(mavenRepoUrlField.getText());

        // Save current pod state before saving configuration
        if (currentPodName != null && !currentPodName.isEmpty()) {
            saveCurrentPodState(currentPodName);
        }

        // Save all pod states - each pod has its own complete configuration
        config.setPodStates(new HashMap<>(podStateMap));

        // Save all broker configurations to the config file
        config.setBrokerConfigs(new ArrayList<>(brokerConfigs));

        return config;
    }

    private TestConfiguration buildConfiguration() {
        TestConfiguration config = new TestConfiguration();
        String keySerializer = resolveSerializer(keySerializerField.getText());
        String valueSerializer = resolveSerializer(valueSerializerField.getText());

        config.setPodNumber("");
        config.setSourceTopicName(sourceTopicField.getText());
        parsePartition(config);
        parseOffset(config);
        config.setSourceKeyDeserializerClass(sourceKeyDeserializerField.getText());
        config.setSourceValueDeserializerClass(searchValueDeserializerField.getText());
        // Save search tab fields
        KafkaBrokerConfig selectedBroker2 = getSelectedBroker();
        if (selectedBroker2 != null) {
            config.setSelectedBrokerName(selectedBroker2.getName());
            config.setBootstrapServers(selectedBroker2.getBootstrapServers());
        }
        config.setSearchTopic(searchTopicComboBox.getValue());
        config.setSearchValueDeserializer(searchValueDeserializerField.getText());
        // Save string representations of field configs
        config.setAutoIncrementFieldsString(autoIncrementFieldsField.getText());
        config.setRotationFieldsString(rotationFieldsField.getText());
        config.setUuidFieldsString(uuidFieldsField.getText());
        config.setMessageHeadersString(messageHeadersField.getText());
        config.setTopicName(pushTopicComboBox.getValue());

        config.setTotalMessageCount(Integer.parseInt(messageCountField.getText()));
        config.setThreadPoolSize(Integer.parseInt(threadPoolSizeField.getText()));
        config.setBatchSize(Integer.parseInt(batchSizeField.getText()));
        config.setMessageTemplate(pushMessageTemplateArea.getText());
        config.setKeySerializerClass(keySerializer);
        config.setValueSerializerClass(valueSerializer);
        config.setProtobufClassName(protobufClassNameField.getText());
        config.setFlatbufClassName(pushFlatbufClassNameField.getText());
        config.setFlatbufMode(pushFlatbufModeCheck.isSelected());
        if (pushFlatbufModeCheck.isSelected()) {
            config.setKeySerializerClass("org.apache.kafka.common.serialization.ByteArraySerializer");
            config.setValueSerializerClass("org.apache.kafka.common.serialization.ByteArraySerializer");
        }
        config.setMavenDependency(mavenDependencyField.getText());
        config.setMavenRepoUrl(mavenRepoUrlField.getText());
        config.setAutoIncrementFields(parseAutoIncrementFields(autoIncrementFieldsField.getText()));
        config.setRotationFields(parseRotationFields(rotationFieldsField.getText()));
        config.setUuidFields(parseUuidFields(uuidFieldsField.getText()));
        config.setMessageHeaders(parseHeaders(messageHeadersField.getText()));

        // Add SSL configuration to additionalKafkaProperties for producer
        KafkaBrokerConfig brokerForSsl = getSelectedBroker();
        if (brokerForSsl != null) {
            Map<String, String> sslProps = buildSslPropertiesForProducer(brokerForSsl);
            if (!sslProps.isEmpty()) {
                config.setAdditionalKafkaProperties(sslProps);
            }
        }

        // Handle partition and key configuration with new radio button structure
        if (partitionKeyAll.isSelected()) {
            // All (auto) - no partition, no key
            config.setTargetPartition(null);
            config.setMessageKeyType("none");
            config.setMessageKeyField(null);
            config.setMessageKeyHardcoded(null);
        } else if (partitionKeySpecific.isSelected()) {
            // Specific partition - partition specified, no key
            try {
                String tpVal2 = targetPartitionField.getValue() != null ? targetPartitionField.getValue() : (targetPartitionField.getEditor().getText() != null ? targetPartitionField.getEditor().getText() : "");
                config.setTargetPartition(Integer.parseInt(tpVal2.trim()));
            } catch (NumberFormatException e) {
                String tpDbg2 = targetPartitionField.getValue() != null ? targetPartitionField.getValue() : (targetPartitionField.getEditor().getText() != null ? targetPartitionField.getEditor().getText() : "");
                logger.warn("Invalid target partition: {}", tpDbg2);
                config.setTargetPartition(null);
            }
            config.setMessageKeyType("none");
            config.setMessageKeyField(null);
            config.setMessageKeyHardcoded(null);
        } else if (partitionKeyField.isSelected()) {
            // Use field from template as key - no partition, key from field
            config.setTargetPartition(null);
            config.setMessageKeyType("field");
            config.setMessageKeyField(keyFieldComboBox.getValue());
            config.setMessageKeyHardcoded(null);
        } else if (partitionKeyConstant.isSelected()) {
            // Constant key - no partition, constant key
            config.setTargetPartition(null);
            config.setMessageKeyType("hardcoded");
            config.setMessageKeyField(null);
            config.setMessageKeyHardcoded(keyConstantField.getText());
        }

        return config;
    }

    private void parsePartition(TestConfiguration config) {
        String input = sourcePartitionField.getText();
        if (input != null && !input.trim().isEmpty()) {
            try { config.setSourcePartition(Integer.parseInt(input.trim())); }
            catch (NumberFormatException ignored) {}
        }
    }

    private void parseOffset(TestConfiguration config) {
        String input = sourceOffsetField.getText();
        if (input != null && !input.trim().isEmpty()) {
            try { config.setSourceOffset(Long.parseLong(input.trim())); }
            catch (NumberFormatException ignored) {}
        }
    }

    private String resolveSerializer(String input) {
        return (input == null || input.trim().isEmpty()) ? DEFAULT_SERIALIZER : input.trim();
    }

    private void applyFullConfiguration(TestConfiguration config) {
        // Global settings
        // Maven dependency is global (common for all pods)
        if (config.getMavenDependency() != null) mavenDependencyField.setText(config.getMavenDependency());
        if (config.getMavenRepoUrl() != null) mavenRepoUrlField.setText(config.getMavenRepoUrl());

        // Load all pod states
        if (config.getPodStates() != null) {
            podStateMap.clear();
            podStateMap.putAll(config.getPodStates());
            logger.info("Loaded {} pod states from configuration", config.getPodStates().size());
        }

        // Select the broker in dropdown (but don't mark as active/connected)
        if (config.getSelectedBrokerName() != null && !config.getSelectedBrokerName().isEmpty()) {
            brokerConfigs.stream()
                .filter(b -> b.getName().equals(config.getSelectedBrokerName()))
                .findFirst()
                .ifPresent(broker -> {
                    // Update the broker's maven dependency to match the configuration
                    if (config.getMavenDependency() != null && !config.getMavenDependency().isEmpty()) {
                        broker.setMavenDependency(config.getMavenDependency());
                        saveBrokerConfigs();
                    }
                    // Select in dropdown only - user must click Connect
                    selectedForWorkComboBox.setValue(broker);
                });
        }
    }

    private void loadDefaultConfiguration() {
        TestConfiguration defaultConfig = new TestConfiguration();
        applyFullConfiguration(defaultConfig);
        keySerializerField.clear();
        valueSerializerField.clear();
        autoIncrementFieldsField.clear();
        rotationFieldsField.clear();
    }

    // ========== ENGINE HELPERS ==========

    private MessageGenerator<String> buildMessageGenerator(TestConfiguration config) {
        return new MessageGenerator<>(config.getMessageTemplate(), config.getAutoIncrementFields(),
                config.getRotationFields(), config.getUuidFields());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaLoadTestEngine createProtobufEngine(TestConfiguration config) throws Exception {
        String serializerClassName = config.getValueSerializerClass();
        String protobufClassName = deriveProtobufClassName(config, serializerClassName);
        ClassLoader cl = searchService.getCustomClassLoader();

        appendToConsole("Serializer class: " + serializerClassName);
        appendToConsole("Derived protobuf class: " + protobufClassName);

        if (cl == null && config.getMavenDependency() != null && !config.getMavenDependency().trim().isEmpty()) {
            appendToConsole("Loading Maven dependency: " + config.getMavenDependency());
            MavenDependencyResolver.MavenDependency dep =
                    MavenDependencyResolver.MavenDependency.parse(config.getMavenDependency());
            cl = mavenResolver.loadDependencyIntoClassLoader(dep.getGroupId(), dep.getArtifactId(), dep.getVersion());
            searchService.setCustomClassLoader(cl);
            appendToConsole("✓ ClassLoader created with Maven dependency");
        } else if (cl != null) {
            appendToConsole("Using existing custom ClassLoader");
        } else {
            appendToConsole("No Maven dependency configured, using default ClassLoader");
        }

        MessageGenerator messageGenerator;
        if (protobufClassName != null) {
            appendToConsole("Loading protobuf class: " + protobufClassName);
            try {
                Class<?> protobufClass = (cl != null)
                        ? Class.forName(protobufClassName, true, cl)
                        : Class.forName(protobufClassName);
                messageGenerator = new com.personal.kafka.pilot.engine.ProtobufMessageGenerator(
                        config.getMessageTemplate(), config.getAutoIncrementFields(), config.getRotationFields(), protobufClass);
                appendToConsole("✓ Protobuf engine ready: " + protobufClassName);
            } catch (ClassNotFoundException e) {
                appendToConsole("✗ Class not found: " + protobufClassName);
                if (cl != null) {
                    appendToConsole("Trying to list available classes in ClassLoader...");
                    appendToConsole("Parent ClassLoader: " + cl.getParent());
                }
                throw new ClassNotFoundException("Could not load protobuf class: " + protobufClassName +
                    ". Make sure the Maven dependency contains this class or provide a custom JAR with protobuf classes.", e);
            }
        } else {
            messageGenerator = buildMessageGenerator(config);
            appendToConsole("✓ String engine ready (no protobuf class configured)");
        }
        appendToConsole("");

        return new KafkaLoadTestEngine(config, messageGenerator, metrics,
                msg -> appendToConsole(String.valueOf(msg)), cl);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private KafkaLoadTestEngine createFlatBuffersEngine(TestConfiguration config) throws Exception {
        String serializerClassName = config.getValueSerializerClass();
        ClassLoader cl = searchService.getCustomClassLoader();

        appendToConsole("Serializer class: " + serializerClassName);

        if (cl == null && config.getMavenDependency() != null && !config.getMavenDependency().trim().isEmpty()) {
            appendToConsole("Loading Maven dependency: " + config.getMavenDependency());
            MavenDependencyResolver.MavenDependency dep =
                    MavenDependencyResolver.MavenDependency.parse(config.getMavenDependency());
            cl = mavenResolver.loadDependencyIntoClassLoader(dep.getGroupId(), dep.getArtifactId(), dep.getVersion());
            searchService.setCustomClassLoader(cl);
            appendToConsole("\u2713 ClassLoader created with Maven dependency");
        } else if (cl != null) {
            appendToConsole("Using existing custom ClassLoader");
        } else {
            appendToConsole("No Maven dependency configured, using default ClassLoader");
        }

        // The POJO ENTITY class is what JSON is bound into and what FlatBufferUtils serializes
        // (exactly like CloudView's AlertingEventService). The generated FlatBuffers Table lives
        // in the sibling ".dto" package. Accept either the ".entity.*" or the ".dto.*" class name.
        Class<?> flatbufClass = null;
        String daoPackage = null;
        String flatbufClassName = config.getFlatbufClassName();
        if (flatbufClassName != null && !flatbufClassName.trim().isEmpty()) {
            ClassLoader loader = cl != null ? cl : Thread.currentThread().getContextClassLoader();
            String configured = flatbufClassName.trim();
            String simpleName = configured.substring(configured.lastIndexOf('.') + 1);
            String pkg = configured.contains(".") ? configured.substring(0, configured.lastIndexOf('.')) : "";

            String entityClassName;
            if (pkg.endsWith(".dto")) {
                String base = pkg.substring(0, pkg.length() - ".dto".length());
                entityClassName = base + ".entity." + simpleName;
                daoPackage = pkg;
            } else if (pkg.endsWith(".entity")) {
                String base = pkg.substring(0, pkg.length() - ".entity".length());
                entityClassName = configured;
                daoPackage = base + ".dto";
            } else {
                entityClassName = configured;
                daoPackage = pkg;
            }

            try {
                flatbufClass = Class.forName(entityClassName, true, loader);
                appendToConsole("\u2713 FlatBuf entity class loaded: " + flatbufClass.getName());
                appendToConsole("\u2713 Dao (generated) package: " + daoPackage);
            } catch (ClassNotFoundException e) {
                // Fall back to whatever the user typed (legacy behaviour) if the entity isn't found.
                try {
                    flatbufClass = Class.forName(configured, true, loader);
                    appendToConsole("\u26a0 Entity class " + entityClassName + " not found; using configured class "
                            + flatbufClass.getName());
                } catch (ClassNotFoundException e2) {
                    appendToConsole("\u26a0 FlatBuf class not found: " + configured + " — sending JSON bytes");
                }
            }
        }

        com.personal.kafka.pilot.engine.MessageGenerator messageGenerator =
                new com.personal.kafka.pilot.engine.FlatBuffersMessageGenerator(
                        config.getMessageTemplate(), config.getAutoIncrementFields(),
                        config.getRotationFields(), flatbufClass, cl, daoPackage);
        appendToConsole("\u2713 FlatBuffers engine ready");
        appendToConsole("");

        return new KafkaLoadTestEngine(config, messageGenerator, metrics,
                msg -> appendToConsole(String.valueOf(msg)), cl);
    }

    private String deriveProtobufClassName(TestConfiguration config, String serializerClassName) {
        if (config.getProtobufClassName() != null && !config.getProtobufClassName().trim().isEmpty()) {
            return config.getProtobufClassName();
        }
        if (serializerClassName.contains("kafka.common.serialization")) return null;

        int lastDot = serializerClassName.lastIndexOf('.');
        String pkg = serializerClassName.substring(0, lastDot).replace(".serializer", ".assets");
        String cls = serializerClassName.substring(lastDot + 1);
        if (cls.endsWith("Serializer")) cls = cls.substring(0, cls.length() - 10);
        return pkg + "." + cls + "Proto$" + cls;
    }

    // ========== UTILITY ==========

    /**
     * Batched console logging: Adds message to queue, flushed periodically.
     * Prevents UI freezing when processing large numbers of messages.
     */
    private void appendToConsole(String message) {
        String podName = getCurrentPodName();
        String timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String prefix = "[" + timestamp + "] [" + podName + "] ";
        consoleLogQueue.offer(prefix + message + "\n");

        // Trigger flush if queue is getting large
        if (consoleLogQueue.size() >= CONSOLE_BATCH_SIZE) {
            flushConsoleQueue();
        }
    }

    /**
     * Batched console logging with explicit pod name.
     */
    private void appendToConsole(String podName, String message) {
        String timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String prefix = "[" + timestamp + "] [" + podName + "] ";
        consoleLogQueue.offer(prefix + message + "\n");

        // Trigger flush if queue is getting large
        if (consoleLogQueue.size() >= CONSOLE_BATCH_SIZE) {
            flushConsoleQueue();
        }
    }

    /**
     * Flushes the console log queue to the UI in a single Platform.runLater() call.
     * Auto-clears console when it exceeds CONSOLE_MAX_LINES threshold.
     */
    private void flushConsoleQueue() {
        if (!consoleAutoFlushEnabled || consoleFlushInProgress.getAndSet(true)) {
            return; // Already flushing or disabled
        }

        // Drain queue and build batch
        StringBuilder batch = new StringBuilder();
        int count = 0;
        String line;
        while ((line = consoleLogQueue.poll()) != null && count < CONSOLE_BATCH_SIZE * 2) {
            batch.append(line);
            count++;
        }

        if (count == 0) {
            consoleFlushInProgress.set(false);
            return;
        }

        String batchText = batch.toString();
        Platform.runLater(() -> {
            try {
                // Check if we need to auto-clear
                int currentLines = consoleOutput.getText().split("\n", -1).length;
                if (currentLines > CONSOLE_MAX_LINES) {
                    // Keep last 1000 lines
                    String text = consoleOutput.getText();
                    int lastNewline = text.lastIndexOf('\n', text.length() - 1000);
                    if (lastNewline > 0) {
                        consoleOutput.setText("--- Log auto-cleared (retained last 1000 lines) ---\n" + text.substring(lastNewline + 1));
                    } else {
                        consoleOutput.clear();
                        consoleOutput.setText("--- Log auto-cleared ---\n");
                    }
                }
                consoleOutput.appendText(batchText);
            } finally {
                consoleFlushInProgress.set(false);
            }
        });
    }

    /**
     * Starts the background console flush scheduler.
     * Call this during initialization.
     */
    private void startConsoleFlushScheduler() {
        if (consoleFlushScheduler != null && !consoleFlushScheduler.isShutdown()) {
            return;
        }
        consoleFlushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "console-flush");
            t.setDaemon(true);
            return t;
        });
        consoleFlushScheduler.scheduleAtFixedRate(this::flushConsoleQueue, CONSOLE_FLUSH_INTERVAL_MS, CONSOLE_FLUSH_INTERVAL_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /**
     * Stops the background console flush scheduler.
     * Call this during shutdown.
     */
    private void stopConsoleFlushScheduler() {
        if (consoleFlushScheduler != null) {
            consoleFlushScheduler.shutdown();
            try {
                if (!consoleFlushScheduler.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS)) {
                    consoleFlushScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                consoleFlushScheduler.shutdownNow();
            }
        }
        // Final flush of any remaining messages
        flushConsoleQueue();
    }

    private String getBootstrapServers() {
        // Use "Selected for Work" dropdown as the source of truth for all operations
        KafkaBrokerConfig broker = selectedForWorkComboBox != null ? selectedForWorkComboBox.getValue() : null;
        if (broker == null) {
            showErrorAlert("No Broker Selected", "Please select a broker from 'Selected for Work' dropdown first.");
            return "";
        }
        return broker.getBootstrapServers().trim()
                .replace("\n", ",").replaceAll(",+", ",").replaceAll(",$", "");
    }

    private void updateProgress() {
        while (engine != null && engine.isRunning()) {
            try {
                Thread.sleep(500);
                Platform.runLater(() -> {
                    double progress = metrics.getProgressPercentage() / 100.0;
                    progressBar.setProgress(progress);
                    statusLabel.setText(String.format("Running (%.1f%% - %.0f msg/s)",
                            metrics.getProgressPercentage(), metrics.getMessagesPerSecond()));
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private List<String> findSerializerClasses(List<File> jarFiles) {
        List<String> classes = new ArrayList<>();
        for (File jarFile : jarFiles) {
            if (!jarFile.getName().endsWith(".jar")) continue;
            try (JarFile jar = new JarFile(jarFile)) {
                classes.addAll(jar.stream()
                        .map(JarEntry::getName)
                        .filter(n -> n.endsWith(".class"))
                        .filter(n -> n.toLowerCase().contains("serializer") || n.toLowerCase().contains("deserializer"))
                        .map(n -> n.replace('/', '.').replace(".class", ""))
                        .collect(Collectors.toList()));
            } catch (Exception e) {
                logger.warn("Could not read JAR: {}", jarFile.getName());
            }
        }
        return classes;
    }

    private List<FieldConfig> parseAutoIncrementFields(String input) {
        if (input == null || input.trim().isEmpty()) return new ArrayList<>();
        return Arrays.stream(input.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(field -> {
                    String[] parts = field.split(":");
                    FieldConfig fc = new FieldConfig();
                    fc.setFieldPath(parts[0].trim());
                    fc.setFieldType(FieldConfig.FieldType.AUTO_INCREMENT);
                    fc.setStartValue(parts.length > 1 ? Long.parseLong(parts[1].trim()) : 1L);
                    fc.setIncrementStep(parts.length > 2 ? Long.parseLong(parts[2].trim()) : 1L);
                    return fc;
                }).collect(Collectors.toList());
    }

    private List<FieldConfig> parseRotationFields(String input) {
        if (input == null || input.trim().isEmpty()) return new ArrayList<>();
        return Arrays.stream(input.split(";"))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(field -> {
                    String[] parts = field.split(":");
                    FieldConfig fc = new FieldConfig();
                    fc.setFieldPath(parts[0].trim());
                    fc.setFieldType(FieldConfig.FieldType.ROTATION);
                    if (parts.length > 1) {
                        fc.setRotationValues(Arrays.stream(parts[1].split(","))
                                .map(String::trim).collect(Collectors.toList()));
                    }
                    return fc;
                }).collect(Collectors.toList());
    }

    private List<FieldConfig> parseUuidFields(String input) {
        if (input == null || input.trim().isEmpty()) return new ArrayList<>();
        return Arrays.stream(input.split(","))
                .map(String::trim).filter(s -> !s.isEmpty())
                .map(fieldName -> {
                    FieldConfig fc = new FieldConfig();
                    fc.setFieldPath(fieldName);
                    fc.setFieldType(FieldConfig.FieldType.UUID);
                    return fc;
                }).collect(Collectors.toList());
    }

    private Map<String, String> parseHeaders(String input) {
        Map<String, String> headers = new LinkedHashMap<>();
        if (input == null || input.trim().isEmpty()) return headers;
        for (String pair : input.split(",")) {
            String trimmed = pair.trim();
            int eq = trimmed.indexOf('=');
            if (eq > 0) {
                headers.put(trimmed.substring(0, eq).trim(), trimmed.substring(eq + 1).trim());
            }
        }
        return headers;
    }

    // Helper methods to convert FieldConfig lists back to strings
    private String convertAutoIncrementFieldsToString(List<FieldConfig> fields) {
        if (fields == null || fields.isEmpty()) return "";
        return fields.stream()
                .map(f -> f.getFieldPath() + ":" + f.getStartValue() + ":" + f.getIncrementStep())
                .collect(Collectors.joining(", "));
    }

    private String convertRotationFieldsToString(List<FieldConfig> fields) {
        if (fields == null || fields.isEmpty()) return "";
        return fields.stream()
                .map(f -> f.getFieldPath() + ":" + f.getRotationValues().stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(",")))
                .collect(Collectors.joining("; "));
    }

    private String convertUuidFieldsToString(List<FieldConfig> fields) {
        if (fields == null || fields.isEmpty()) return "";
        return fields.stream()
                .map(FieldConfig::getFieldPath)
                .collect(Collectors.joining(", "));
    }

    private void showErrorAlert(String header, String content) {
        Alert alert = new Alert(Alert.AlertType.ERROR);
        alert.setTitle("Error");
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    private void showInfoAlert(String header, String content) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Information");
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    private void showWarningAlert(String header, String content) {
        Alert alert = new Alert(Alert.AlertType.WARNING);
        alert.setTitle("Warning");
        alert.setHeaderText(header);
        alert.setContentText(content);
        alert.showAndWait();
    }

    // ========== VERIFY TAB HANDLERS ==========

    @SuppressWarnings("unchecked")
    private void autoFillVerifyTab() {
        if (engine == null) {
            appendToConsole("[AutoFill] No engine — run a push test first.");
            return;
        }
        Map<Integer, Long> offsets = engine.getPartitionMaxOffsets();
        appendToConsole("[AutoFill] Partition map size: " + (offsets == null ? "null" : offsets.size()));
        if (offsets == null || offsets.isEmpty()) {
            appendToConsole("[AutoFill] No partition data yet — push may not have completed or no acks received.");
            return;
        }

        String topic = pushTopicComboBox.getValue();
        if (topic != null && !topic.trim().isEmpty()) {
            verifyTopicComboBox.setValue(topic.trim());
        }

        StringBuilder sb = new StringBuilder();
        offsets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> sb.append(e.getKey()).append("=").append(e.getValue() + 1).append("\n"));
        verifyPartitionOffsetsArea.setText(sb.toString().trim());
        appendToConsole("[AutoFill] Done — " + offsets.size() + " partition(s) filled.");
    }

    @FXML
    private void handleAutoFillVerifyOffsets() {
        if (!consoleVisible) handleToggleConsole();
        autoFillVerifyTab();
    }

    @FXML
    private void handleClearVerifyOffsets() {
        verifyPartitionOffsetsArea.clear();
    }

    @FXML
    private void handleApplyCapturedOffsets() {
        // Apply offsets stored in memory (useful when UI was stuck during search/peek)
        if (lastSearchPartitionOffsets.isEmpty()) {
            verifyStatusLabel.setText("⚠ No captured offsets available");
            appendToConsole("[ApplyOffsets] No offsets in memory — run a search or peek first");
            return;
        }

        StringBuilder sb = new StringBuilder();
        lastSearchPartitionOffsets.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> sb.append(e.getKey()).append("=").append(e.getValue() + 1).append("\n"));
        verifyPartitionOffsetsArea.setText(sb.toString().trim());

        // Also set topic if available
        if (lastSearchTopic != null && (verifyTopicComboBox.getValue() == null || verifyTopicComboBox.getValue().isEmpty())) {
            verifyTopicComboBox.setValue(lastSearchTopic);
        }

        // Apply message template if available
        String template = lastSearchMessageTemplate.get();
        if (template != null && !template.isEmpty()) {
            pushMessageTemplateArea.setText(template);
        }

        verifyStatusLabel.setText("\u2713 Applied " + lastSearchPartitionOffsets.size() + " captured offset(s)");
        appendToConsole("[ApplyOffsets] Applied " + lastSearchPartitionOffsets.size() + " partition offset(s) from memory");
    }

    @FXML
    private void handleAddConsumerGroup() {
        String group = availableConsumerGroupsCombo.getValue();
        if (group == null || group.trim().isEmpty()) {
            verifyStatusLabel.setText("⚠ Enter or select a consumer group");
            appendToConsole("[AddGroup] ERROR: No group entered");
            return;
        }
        group = group.trim();

        // Check if already in selected list
        if (selectedConsumerGroupsList.getItems().contains(group)) {
            verifyStatusLabel.setText("⚠ Group already selected: " + group);
            appendToConsole("[AddGroup] ERROR: Group already selected");
            return;
        }

        // Add to selected list
        selectedConsumerGroupsList.getItems().add(group);

        // Remove from available combo if present
        if (availableConsumerGroupsCombo.getItems().contains(group)) {
            availableConsumerGroupsCombo.getItems().remove(group);
        }

        // Clear the combo editor
        availableConsumerGroupsCombo.setValue(null);
        availableConsumerGroupsCombo.getEditor().clear();

        updateVerifyStatusLabel();
    }

    @FXML
    private void handleFetchConsumerGroups() {
        String topicValue = verifyTopicComboBox.getValue();
        if (topicValue == null || topicValue.trim().isEmpty()) {
            verifyStatusLabel.setText("⚠ Select topic first");
            appendToConsole("[FetchGroups] ERROR: No topic selected");
            return;
        }
        final String topic = topicValue.trim();

        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            verifyStatusLabel.setText("⚠ Not connected to Kafka");
            appendToConsole("[FetchGroups] ERROR: Not connected to Kafka (no active pod)");
            return;
        }

        verifyStatusLabel.setText("⏳ Fetching consumer groups for topic...");
        appendToConsole("[FetchGroups] Starting...");

        // Capture current pod name before starting thread
        final String podName = currentPodName;

        // Run in background thread to avoid blocking UI
        new Thread(() -> {
            try {
                java.util.Set<String> groupsForTopic = new java.util.HashSet<>();
                boolean wasFromFile = false;

                // Step 1: Try to read from file first (memory-efficient)
                List<ConsumerGroupInfo> groupsFromFile = readGroupsForTopicFromFile(podName, topic);

                if (groupsFromFile != null && !groupsFromFile.isEmpty()) {
                    // File cache hit - use data from file
                    wasFromFile = true;
                    for (ConsumerGroupInfo info : groupsFromFile) {
                        groupsForTopic.add(info.groupId);
                    }
                    appendToConsole("[FetchGroups] Loaded " + groupsForTopic.size() + " group(s) from file cache");
                } else {
                    // Step 2: File miss or expired - need to fetch from Kafka and update file
                    appendToConsole("[FetchGroups] File cache miss - fetching all consumer groups from Kafka...");

                    // Fetch all consumer groups for this pod (this updates the file)
                    Map<String, List<ConsumerGroupInfo>> allGroups = fetchAllConsumerGroupsForPodAndCacheToFile(podName, client);

                    // Get groups for this specific topic
                    List<ConsumerGroupInfo> topicGroups = allGroups.get(topic);
                    if (topicGroups != null) {
                        for (ConsumerGroupInfo info : topicGroups) {
                            groupsForTopic.add(info.groupId);
                        }
                    }
                    appendToConsole("[FetchGroups] Fetched " + groupsForTopic.size() + " group(s) from Kafka and saved to file");
                }

                final java.util.Set<String> finalGroups = groupsForTopic;
                final boolean finalWasFromFile = wasFromFile;
                Platform.runLater(() -> {
                    // Clear existing available items but keep selected
                    availableConsumerGroupsCombo.getItems().clear();

                    // Filter out already selected groups from available list
                    java.util.Set<String> selected = new java.util.HashSet<>(selectedConsumerGroupsList.getItems());
                    java.util.List<String> availableGroups = finalGroups.stream()
                            .filter(g -> !selected.contains(g))
                            .sorted()
                            .collect(java.util.stream.Collectors.toList());

                    if (finalGroups.isEmpty()) {
                        verifyStatusLabel.setText("ℹ No consumer groups found for topic: " + topic);
                        appendToConsole("[FetchGroups] Completed: 0 groups");
                    } else {
                        availableConsumerGroupsCombo.getItems().addAll(availableGroups);
                        verifyStatusLabel.setText("✓ Found " + finalGroups.size() + " group(s)" + (finalWasFromFile ? " (from file)" : " (from Kafka)"));
                        appendToConsole("[FetchGroups] Completed: " + finalGroups.size() + " groups");
                    }
                });

            } catch (Exception e) {
                Platform.runLater(() -> {
                    verifyStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[FetchGroups] ERROR: " + e.getMessage());
                });
                logger.error("[FetchGroups] Error", e);
            }
        }).start();
    }

    @FXML
    private void handleStartVerification() {
        boolean anyRunning = activeVerifiers.stream().anyMatch(KafkaConsumerVerifier::isRunning);
        if (anyRunning) {
            appendToConsole("Verification already running.");
            return;
        }

        String topic = verifyTopicComboBox.getValue();
        if (topic != null) topic = topic.trim();
        List<String> groups = new ArrayList<>(selectedConsumerGroupsList.getItems());
        String offsetsText = verifyPartitionOffsetsArea.getText().trim();

        if (topic.isEmpty()) { verifyStatusLabel.setText("⚠ Topic is required"); appendToConsole("[Verify] ERROR: Topic required"); return; }
        if (groups.isEmpty()) { verifyStatusLabel.setText("⚠ Consumer group(s) required"); appendToConsole("[Verify] ERROR: Consumer group(s) required"); return; }
        if (offsetsText.isEmpty()) { verifyStatusLabel.setText("⚠ Partition offsets are required"); appendToConsole("[Verify] ERROR: Partition offsets required"); return; }

        long timeoutMs;
        try {
            String t = verifyTimeoutField.getText().trim();
            timeoutMs = (t.isEmpty() ? 10 : Long.parseLong(t)) * 60_000L;
        } catch (NumberFormatException e) {
            verifyStatusLabel.setText("⚠ Invalid timeout value"); appendToConsole("[Verify] ERROR: Invalid timeout"); return;
        }

        List<KafkaConsumerVerifier.PartitionTarget> targets = new ArrayList<>();
        for (String line : offsetsText.split("\n")) {
            line = line.trim();
            if (line.isEmpty()) continue;
            String[] parts = line.split("=");
            if (parts.length != 2) {
                verifyStatusLabel.setText("⚠ Invalid format on line: " + line);
                appendToConsole("[Verify] ERROR: Invalid offset format");
                return;
            }
            try {
                int partition = Integer.parseInt(parts[0].trim());
                long offset = Long.parseLong(parts[1].trim());
                targets.add(new KafkaConsumerVerifier.PartitionTarget(partition, offset));
            } catch (NumberFormatException e) {
                verifyStatusLabel.setText("⚠ Invalid numbers on line: " + line);
                appendToConsole("[Verify] ERROR: Invalid partition/offset numbers");
                return;
            }
        }

        activeVerifiers.clear();
        verifyStatusLabel.setText("⏳ Running " + groups.size() + " group(s)...");
        verifyStartButton.setDisable(true);
        verifyStopButton.setDisable(false);

        if (!consoleVisible) handleToggleConsole();
        appendToConsole("[Verify] Started: " + groups.size() + " group(s)");

        String bootstrapServers = getBootstrapServers();
        java.util.concurrent.atomic.AtomicInteger remaining = new java.util.concurrent.atomic.AtomicInteger(groups.size());

        for (String group : groups) {
            KafkaConsumerVerifier v = new KafkaConsumerVerifier(bootstrapServers, topic, group, targets,
                    msg -> appendToConsole("[" + group + "] " + msg), timeoutMs);
            activeVerifiers.add(v);
            new Thread(() -> {
                try {
                    v.run();
                } finally {
                    if (remaining.decrementAndGet() == 0) {
                        Platform.runLater(() -> {
                            verifyStartButton.setDisable(false);
                            verifyStopButton.setDisable(true);
                            verifyStatusLabel.setText("✓ All groups done");
                            appendToConsole("[Verify] Completed");
                        });
                    }
                }
            }).start();
        }
    }

    @FXML
    private void handleStopVerification() {
        activeVerifiers.forEach(KafkaConsumerVerifier::stop);
        verifyStatusLabel.setText("Stopping...");
    }

    // ========== TOPIC MANAGEMENT ==========

    /**
     * Refreshes topic dropdowns from current pod's topics.
     * Called when connecting or when pod changes.
     */
    private void refreshTopicsTabDropdowns() {
        String currentPod = currentPodName;
        if (currentPod == null || !podTopics.containsKey(currentPod)) {
            return;
        }

        List<String> topics = podTopics.get(currentPod);
        Platform.runLater(() -> {
            describeTopicComboBox.getItems().setAll(topics);
            updateTopicComboBox.getItems().setAll(topics);
        });
    }

    @FXML
    private void handleDescribeTopic() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            describeTopicStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = describeTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            describeTopicStatusLabel.setText("⚠ Select or enter topic name");
            return;
        }
        topicName = topicName.trim();

        final String finalTopicName = topicName;
        describeTopicStatusLabel.setText("⏳ Describing...");
        appendToConsole("[Topics] Describing topic: " + finalTopicName);

        new Thread(() -> {
            try {
                org.apache.kafka.common.config.ConfigResource resource =
                        new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, finalTopicName);

                java.util.Map<String, org.apache.kafka.clients.admin.TopicDescription> description =
                        client.describeTopics(java.util.Collections.singletonList(finalTopicName))
                                .allTopicNames().get(10, java.util.concurrent.TimeUnit.SECONDS);

                org.apache.kafka.clients.admin.Config config =
                        client.describeConfigs(java.util.Collections.singleton(resource))
                                .all().get(10, java.util.concurrent.TimeUnit.SECONDS).get(resource);

                StringBuilder sb = new StringBuilder();
                sb.append("=== Topic: ").append(finalTopicName).append(" ===\n\n");

                org.apache.kafka.clients.admin.TopicDescription topicDesc = description.get(finalTopicName);
                if (topicDesc != null) {
                    sb.append("Partitions: ").append(topicDesc.partitions().size()).append("\n");
                    sb.append("Replication Factor: ").append(topicDesc.partitions().get(0).replicas().size()).append("\n\n");

                    sb.append("Partition Details:\n");
                    for (org.apache.kafka.common.TopicPartitionInfo partition : topicDesc.partitions()) {
                        sb.append(String.format("  Partition %d: Leader=%s, Replicas=%s, ISR=%s\n",
                                partition.partition(),
                                partition.leader().idString(),
                                partition.replicas().stream().map(n -> n.idString()).collect(java.util.stream.Collectors.joining(",")),
                                partition.isr().stream().map(n -> n.idString()).collect(java.util.stream.Collectors.joining(","))));
                    }
                }

                sb.append("\nConfiguration:\n");
                for (org.apache.kafka.clients.admin.ConfigEntry entry : config.entries()) {
                    if (!entry.isDefault()) {
                        sb.append(String.format("  %s=%s\n", entry.name(), entry.value()));
                    }
                }

                String result = sb.toString();
                
                // Fetch partition offsets
                List<PartitionOffsetData> offsetData = fetchPartitionOffsets(getBootstrapServers(), finalTopicName);
                
                Platform.runLater(() -> {
                    topicDetailsArea.setText(result);
                    partitionOffsetTable.getItems().clear();
                    partitionOffsetTable.getItems().addAll(offsetData);
                    if (topicDetailsPane != null) {
                        topicDetailsPane.setExpanded(true);
                    }
                    describeTopicStatusLabel.setText("✓ Described: " + finalTopicName);
                    appendToConsole("[Topics] Described topic: " + finalTopicName + " with " + offsetData.size() + " partitions");
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    describeTopicStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[Topics] ERROR describing topic: " + e.getMessage());
                });
            }
        }).start();
    }

    @FXML
    private void handleUpdateTopicConfig() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            updateTopicStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = updateTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            updateTopicStatusLabel.setText("⚠ Topic name required");
            return;
        }
        topicName = topicName.trim();

        String configText = updateTopicConfigsArea.getText().trim();
        if (configText.isEmpty()) {
            updateTopicStatusLabel.setText("⚠ Enter at least one config");
            return;
        }

        // Parse configs from text area
        java.util.Map<String, String> configs = parseConfigsFromText(configText);
        if (configs.isEmpty()) {
            updateTopicStatusLabel.setText("⚠ No valid configs found");
            return;
        }

        updateTopicStatusLabel.setText("⏳ Updating config...");
        appendToConsole("[Topics] Updating config for: " + topicName + " with " + configs.size() + " configs");

        final String finalTopicName = topicName;
        new Thread(() -> {
            try {
                // Use deprecated alterConfigs API which is simpler and works reliably
                java.util.Map<org.apache.kafka.common.config.ConfigResource, org.apache.kafka.clients.admin.Config> configMap =
                        new java.util.HashMap<>();

                java.util.List<org.apache.kafka.clients.admin.ConfigEntry> entries = new java.util.ArrayList<>();
                for (java.util.Map.Entry<String, String> entry : configs.entrySet()) {
                    entries.add(new org.apache.kafka.clients.admin.ConfigEntry(entry.getKey(), entry.getValue()));
                }

                org.apache.kafka.common.config.ConfigResource resource =
                        new org.apache.kafka.common.config.ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC, finalTopicName);
                org.apache.kafka.clients.admin.Config config = new org.apache.kafka.clients.admin.Config(entries);
                configMap.put(resource, config);

                client.alterConfigs(configMap).all().get(30, java.util.concurrent.TimeUnit.SECONDS);

                Platform.runLater(() -> {
                    updateTopicStatusLabel.setText("✓ Config updated: " + finalTopicName);
                    appendToConsole("[Topics] Updated config for: " + finalTopicName);
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    updateTopicStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[Topics] ERROR updating config: " + e.getMessage());
                });
            }
        }).start();
    }

    @FXML
    private void handleCreateTopic() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            createTopicStatusLabel.setText("⚠ Not connected");
            appendToConsole("[Topics] ERROR: Not connected to Kafka");
            return;
        }

        String topicName = newTopicNameField.getText().trim();
        if (topicName.isEmpty()) {
            createTopicStatusLabel.setText("⚠ Topic name required");
            return;
        }

        int partitions, replication;
        try {
            partitions = Integer.parseInt(newTopicPartitionsField.getText().trim());
            replication = Integer.parseInt(newTopicReplicationField.getText().trim());
        } catch (NumberFormatException e) {
            createTopicStatusLabel.setText("⚠ Invalid partitions/replication");
            return;
        }

        // Parse optional configs
        String configText = createTopicConfigsArea.getText().trim();
        java.util.Map<String, String> configs = parseConfigsFromText(configText);

        createTopicStatusLabel.setText("⏳ Creating...");
        appendToConsole("[Topics] Creating topic: " + topicName + " (" + partitions + " partitions, " + replication + " replicas)");

        new Thread(() -> {
            try {
                org.apache.kafka.clients.admin.NewTopic newTopic = new org.apache.kafka.clients.admin.NewTopic(topicName, partitions, (short) replication)
                        .configs(configs);

                client.createTopics(java.util.Collections.singleton(newTopic)).all().get(30, java.util.concurrent.TimeUnit.SECONDS);

                Platform.runLater(() -> {
                    createTopicStatusLabel.setText("✓ Created: " + topicName);
                    appendToConsole("[Topics] Created topic: " + topicName);
                    // Refresh dropdowns
                    refreshTopicsTabDropdowns();
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    createTopicStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[Topics] ERROR creating topic: " + e.getMessage());
                });
            }
        }).start();
    }

    @FXML
    private void handleDeleteTopic() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            describeTopicStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = describeTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            describeTopicStatusLabel.setText("⚠ Select or enter topic name");
            return;
        }
        topicName = topicName.trim();

        // Confirmation dialog
        final String finalTopicName = topicName;
        javafx.scene.control.Alert confirm = new javafx.scene.control.Alert(javafx.scene.control.Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Delete Topic");
        confirm.setHeaderText("Delete topic: " + finalTopicName + "?");
        confirm.setContentText("This action cannot be undone.");

        confirm.showAndWait().ifPresent(response -> {
            if (response == javafx.scene.control.ButtonType.OK) {
                describeTopicStatusLabel.setText("⏳ Deleting...");
                appendToConsole("[Topics] Deleting topic: " + finalTopicName);

                new Thread(() -> {
                    try {
                        client.deleteTopics(java.util.Collections.singleton(finalTopicName)).all().get(30, java.util.concurrent.TimeUnit.SECONDS);

                        Platform.runLater(() -> {
                            describeTopicStatusLabel.setText("✓ Deleted: " + finalTopicName);
                            appendToConsole("[Topics] Deleted topic: " + finalTopicName);
                            describeTopicComboBox.setValue(null);
                            // Refresh dropdowns
                            refreshTopicsTabDropdowns();
                        });
                    } catch (Exception e) {
                        Platform.runLater(() -> {
                            describeTopicStatusLabel.setText("⚠ Error: " + e.getMessage());
                            appendToConsole("[Topics] ERROR deleting topic: " + e.getMessage());
                        });
                    }
                }).start();
            }
        });
    }

    
    /**
     * Fetches partition offset information for a topic using a temporary consumer.
     */
    private List<PartitionOffsetData> fetchPartitionOffsets(String bootstrapServers, String topic) throws Exception {
        List<PartitionOffsetData> result = new ArrayList<>();
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "offset-fetch-" + topic + "-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        if (searchService.getCustomClassLoader() != null) {
            Thread.currentThread().setContextClassLoader(searchService.getCustomClassLoader());
        }

        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {
            // Get partition info
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                throw new Exception("Topic not found: " + topic);
            }

            // Create TopicPartition objects
            List<TopicPartition> topicPartitions = partitions.stream()
                .map(pi -> new TopicPartition(topic, pi.partition()))
                .collect(Collectors.toList());

            // Get beginning and end offsets
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);

            // Create offset data for each partition
            for (PartitionInfo partition : partitions) {
                TopicPartition tp = new TopicPartition(topic, partition.partition());
                Long earliest = beginningOffsets.get(tp);
                Long latest = endOffsets.get(tp);
                
                if (earliest != null && latest != null) {
                    result.add(new PartitionOffsetData(partition.partition(), earliest, latest));
                }
            }
        }
        
        // Sort by partition number
        result.sort((a, b) -> Integer.compare(a.getPartition(), b.getPartition()));
        return result;
    }

    /**
     * Parses config text (key=value, one per line) into a map.
     */
    private java.util.Map<String, String> parseConfigsFromText(String text) {
        java.util.Map<String, String> configs = new java.util.HashMap<>();
        if (text == null || text.trim().isEmpty()) {
            return configs;
        }

        for (String line : text.split("\n")) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;
            int eq = line.indexOf('=');
            if (eq > 0) {
                String key = line.substring(0, eq).trim();
                String value = line.substring(eq + 1).trim();
                if (!key.isEmpty()) {
                    configs.put(key, value);
                }
            }
        }
        return configs;
    }

    /**
     * Initializes the config names lists with click-to-copy functionality.
     * Called during UI initialization.
     */
    private void initTopicConfigNamesList() {
        // Update section config list
        topicConfigNamesList.setOnMouseClicked(event -> {
            String selected = topicConfigNamesList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                copyConfigToTextArea(selected, updateTopicConfigsArea, updateTopicStatusLabel);
            }
        });

        // Create section config list
        createTopicConfigNamesList.setOnMouseClicked(event -> {
            String selected = createTopicConfigNamesList.getSelectionModel().getSelectedItem();
            if (selected != null) {
                copyConfigToTextArea(selected, createTopicConfigsArea, createTopicStatusLabel);
            }
        });
    }

    private void startMetricsTicker() {
        metricsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jvm-metrics-ticker");
            t.setDaemon(true);
            return t;
        });
        metricsScheduler.scheduleAtFixedRate(() -> {
            MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            long usedMb = heap.getUsed() / (1024 * 1024);
            long maxMb = heap.getMax() / (1024 * 1024);
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            int threadCount = threadBean.getThreadCount();
            double cpuLoad = -1;
            try {
                com.sun.management.OperatingSystemMXBean osBean =
                        (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                cpuLoad = osBean.getProcessCpuLoad() * 100.0;
            } catch (Exception ignored) {}
            long gcCount = ManagementFactory.getGarbageCollectorMXBeans().stream()
                    .mapToLong(GarbageCollectorMXBean::getCollectionCount)
                    .filter(c -> c >= 0).sum();
            final String heapStr = usedMb + " MB / " + maxMb + " MB";
            final String threadStr = String.valueOf(threadCount);
            final String cpuStr = cpuLoad >= 0 ? String.format("%.1f%%", cpuLoad) : "n/a";
            final String gcStr = gcCount + " collections";
            final String heapColor = usedMb > (maxMb * 0.85) ? "#e74c3c" : "#27ae60";
            Platform.runLater(() -> {
                metricsHeapLabel.setText(heapStr);
                metricsHeapLabel.setStyle("-fx-text-fill: " + heapColor + "; -fx-font-size: 10px; -fx-font-weight: bold;");
                metricsThreadsLabel.setText(threadStr);
                metricsCpuLabel.setText(cpuStr);
                metricsGcLabel.setText(gcStr);
            });
        }, 1, 3, TimeUnit.SECONDS);
    }

    /**
     * Helper to copy config name to text area with click-to-copy behavior.
     */
    private void copyConfigToTextArea(String configName, TextArea targetArea, Label statusLabel) {
        // Copy to clipboard
        javafx.scene.input.Clipboard clipboard = javafx.scene.input.Clipboard.getSystemClipboard();
        javafx.scene.input.ClipboardContent content = new javafx.scene.input.ClipboardContent();
        content.putString(configName + "=");
        clipboard.setContent(content);

        // Show feedback
        statusLabel.setText("✓ Copied: " + configName + "=");

        // Also append to the configs text area
        String current = targetArea.getText();
        if (!current.isEmpty() && !current.endsWith("\n")) {
            current += "\n";
        }
        targetArea.setText(current + configName + "=\n");
        targetArea.positionCaret(targetArea.getText().length());
    }

    // ========== CONSUMER GROUP MANAGEMENT ==========

    /**
     * Refreshes Consumer Groups tab dropdowns from current pod's topics.
     */
    private void refreshConsumerGroupsTabDropdowns() {
        String currentPod = currentPodName;
        if (currentPod == null || !podTopics.containsKey(currentPod)) {
            return;
        }

        List<String> topics = podTopics.get(currentPod);
        Platform.runLater(() -> {
            cgTopicComboBox.getItems().setAll(topics);
            cgLagTopicComboBox.getItems().setAll(topics);
            cgResetTopicComboBox.getItems().setAll(topics);
        });
    }

    @FXML
    private void handleLoadConsumerGroupsForTopic() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            cgDescribeStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = cgTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            cgDescribeStatusLabel.setText("⚠ Select a topic first");
            return;
        }
        topicName = topicName.trim();

        final String finalTopicName = topicName;
        // Capture current pod name before starting thread
        final String podName = currentPodName;

        // Check if consumer groups are already being fetched for this pod
        if (podsFetchingConsumerGroups.contains(podName)) {
            String message = "Consumer groups are already being fetched for pod " + podName + ". Please wait...";
            cgDescribeStatusLabel.setText("⏳ " + message);
            appendToConsole(podName, "[Consumer Groups] " + message);
            logger.info("[Consumer Groups] Fetch already in progress for pod {}, skipping duplicate request", podName);
            return;
        }

        cgDescribeStatusLabel.setText("⏳ Loading groups...");
        appendToConsole(podName, "[Consumer Groups] Loading groups for topic: " + finalTopicName);

        new Thread(() -> {
            try {
                // Check file cache first
                boolean cacheValid = isCgCacheValid(podName);

                if (!cacheValid) {
                    if (podsFetchingConsumerGroups.contains(podName)) {
                        // Another thread is already fetching — wait for it
                        Platform.runLater(() -> cgDescribeStatusLabel.setText("⏳ Fetching in progress, waiting..."));
                        int attempts = 0;
                        while (podsFetchingConsumerGroups.contains(podName) && attempts < 30) {
                            Thread.sleep(1000);
                            attempts++;
                        }
                        if (!isCgCacheValid(podName)) {
                            Platform.runLater(() -> cgDescribeStatusLabel.setText("⚠ Fetch timeout - please try again"));
                            return;
                        }
                    } else {
                        podsFetchingConsumerGroups.add(podName);
                        try {
                            appendToConsole(podName, "[Consumer Groups] Cache miss/expired — fetching from Kafka...");
                            Map<String, List<ConsumerGroupInfo>> fresh = fetchAllConsumerGroupsForPod(client, null);
                            writeCgCacheToFile(podName, fresh);
                            fresh.clear();
                        } finally {
                            podsFetchingConsumerGroups.remove(podName);
                        }
                    }
                } else {
                    appendToConsole(podName, "[Consumer Groups] Using file cache...");
                }

                // Read only the requested topic from the file — minimal memory
                List<ConsumerGroupInfo> groupsForTopic = readGroupsForTopicFromFile(podName, finalTopicName);
                Set<String> groupIds = new HashSet<>();
                if (groupsForTopic != null) {
                    for (ConsumerGroupInfo cgInfo : groupsForTopic) groupIds.add(cgInfo.groupId);
                }
                groupsForTopic = null; // release immediately

                final List<String> finalGroupIds = new ArrayList<>(groupIds);
                final boolean wasCached = cacheValid;
                Platform.runLater(() -> {
                    cgConsumerGroupComboBox.getItems().clear();
                    if (!finalGroupIds.isEmpty()) {
                        cgConsumerGroupComboBox.getItems().addAll(finalGroupIds);
                        cgConsumerGroupComboBox.setValue(finalGroupIds.get(0));
                        cgDescribeStatusLabel.setText("✓ Found " + finalGroupIds.size() + " group(s)" + (wasCached ? " (cached)" : ""));
                        appendToConsole(podName, "[Consumer Groups] Found " + finalGroupIds.size() + " group(s) for topic " + finalTopicName);
                    } else {
                        cgDescribeStatusLabel.setText("⚠ No consumer group found");
                        appendToConsole(podName, "[Consumer Groups] No consumer groups found for topic " + finalTopicName);
                    }
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    cgDescribeStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole(podName, "[Consumer Groups] ERROR loading groups: " + e.getMessage());
                });
            }
        }).start();
    }

    @FXML
    private void handleDescribeConsumerGroup() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            cgDescribeStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = cgTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            cgDescribeStatusLabel.setText("⚠ Select a topic");
            return;
        }
        topicName = topicName.trim();

        String selectedGroupId = cgConsumerGroupComboBox.getValue();

        final String finalTopicName = topicName;
        final String finalGroupId = selectedGroupId;
        // Capture current pod name before starting thread
        final String podName = currentPodName;
        cgDescribeStatusLabel.setText("⏳ Describing...");
        appendToConsole("[Consumer Groups] Describing for topic: " + finalTopicName +
                       (finalGroupId != null ? ", group: " + finalGroupId : ""));

        new Thread(() -> {
            try {
                // describeConsumerGroupForTopic caches ALL consumer groups for ALL topics in the pod
                ConsumerGroupInfo info = describeConsumerGroupForTopic(finalTopicName, client, podName);

                if (info != null) {
                    // If a specific group was selected, verify it matches
                    if (finalGroupId != null && !finalGroupId.trim().isEmpty() &&
                        !info.groupId.equals(finalGroupId.trim())) {
                        // The selected group doesn't match the cached one - fetch directly
                        info = describeSpecificConsumerGroup(finalGroupId.trim(), finalTopicName, client);
                    }

                    final ConsumerGroupInfo finalInfo = info;
                    Platform.runLater(() -> {
                        displayConsumerGroupInfo(finalInfo);
                        cgDescribeStatusLabel.setText("✓ Group: " + finalInfo.groupId);
                        if (cgDetailsPane != null) {
                            cgDetailsPane.setExpanded(true);
                        }
                        appendToConsole("[Consumer Groups] Found group: " + finalInfo.groupId);
                    });
                } else {
                    Platform.runLater(() -> {
                        cgDescribeStatusLabel.setText("⚠ No consumer group found");
                        cgDetailsArea.setText("No consumer group found for topic: " + finalTopicName);
                        if (cgDetailsPane != null) {
                            cgDetailsPane.setExpanded(true);
                        }
                    });
                }
            } catch (Exception e) {
                Platform.runLater(() -> {
                    cgDescribeStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[Consumer Groups] ERROR: " + e.getMessage());
                });
            }
        }).start();
    }

    /**
     * Describes and caches ALL consumer groups for ALL topics in the pod.
     * Returns the ConsumerGroupInfo for the requested topic.
     * Structure: pod -> topic -> ConsumerGroupInfo
     */
    private ConsumerGroupInfo describeConsumerGroupForTopic(String topicName,
                                                            org.apache.kafka.clients.admin.AdminClient client,
                                                            String podName) throws Exception {
        if (!isCgCacheValid(podName)) {
            Map<String, List<ConsumerGroupInfo>> fresh = fetchAllConsumerGroupsForPod(client, null);
            writeCgCacheToFile(podName, fresh);
            fresh.clear();
            logger.info("[CG File Storage] Refreshed file cache for pod {}", podName);
        } else {
            logger.debug("[CG File Storage] Using file cache for pod {}", podName);
        }
        List<ConsumerGroupInfo> groups = readGroupsForTopicFromFile(podName, topicName);
        return groups != null && !groups.isEmpty() ? groups.get(0) : null;
    }

    /**
     * Describes a specific consumer group by its groupId for a given topic.
     * Used when the user selects a specific group that's different from the cached one.
     */
    private ConsumerGroupInfo describeSpecificConsumerGroup(String groupId, String topicName,
                                                              org.apache.kafka.clients.admin.AdminClient client) throws Exception {
        java.util.Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> descriptions =
            client.describeConsumerGroups(java.util.Collections.singleton(groupId))
                .all().get(20, java.util.concurrent.TimeUnit.SECONDS);

        org.apache.kafka.clients.admin.ConsumerGroupDescription desc = descriptions.get(groupId);
        if (desc == null || desc.members().isEmpty()) {
            return null;
        }

        // Check if this group consumes the specified topic
        boolean consumesTopic = false;
        for (org.apache.kafka.clients.admin.MemberDescription member : desc.members()) {
            for (org.apache.kafka.common.TopicPartition tp : member.assignment().topicPartitions()) {
                if (tp.topic().equals(topicName)) {
                    consumesTopic = true;
                    break;
                }
            }
            if (consumesTopic) break;
        }

        if (!consumesTopic) {
            return null;
        }

        // Create ConsumerGroupInfo
        ConsumerGroupInfo info = new ConsumerGroupInfo(groupId, topicName);

        // Add all members that consume this topic
        for (org.apache.kafka.clients.admin.MemberDescription m : desc.members()) {
            ConsumerMemberInfo mi = new ConsumerMemberInfo(
                m.consumerId(), m.clientId(), m.host()
            );
            for (org.apache.kafka.common.TopicPartition t : m.assignment().topicPartitions()) {
                if (t.topic().equals(topicName)) {
                    mi.assignedPartitions.add(t.partition() + "");
                }
            }
            if (!mi.assignedPartitions.isEmpty()) {
                info.members.add(mi);
            }
        }

        // Note: Lag is NOT fetched here - it should be fetched at runtime only
        return info;
    }

    /**
     * Fetches all consumer groups and builds a map of topic -> ConsumerGroupInfo
     * for all topics consumed by any consumer group in the pod.
     * Note: This method does NOT fetch lag - lag should be fetched at runtime only.
     *
     * Optimized for 1000+ groups: 25 threads, batch API calls (25 groups per batch),
     * and aggressive progress reporting.
     *
     * @param client Kafka AdminClient
     * @param topics Optional list of topics to filter by (null for all topics)
     * @return Map of topic -> ConsumerGroupInfo (without lag)
     */
    private static final int CONSUMER_GROUP_BATCH_SIZE = 25;
    private static final int CONSUMER_GROUP_THREAD_POOL = 25;

    private Map<String, List<ConsumerGroupInfo>> fetchAllConsumerGroupsForPod(
            org.apache.kafka.clients.admin.AdminClient client,
            List<String> topics) throws Exception {

        Map<String, List<ConsumerGroupInfo>> topicToGroupsMap = new ConcurrentHashMap<>();
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        java.util.Collection<org.apache.kafka.clients.admin.ConsumerGroupListing> groupListings =
            client.listConsumerGroups().all().get(40, java.util.concurrent.TimeUnit.SECONDS);

        List<String> allGroupIds = groupListings.stream()
            .map(org.apache.kafka.clients.admin.ConsumerGroupListing::groupId)
            .collect(Collectors.toList());

        int totalGroups = allGroupIds.size();
        int totalBatches = (totalGroups + CONSUMER_GROUP_BATCH_SIZE - 1) / CONSUMER_GROUP_BATCH_SIZE;

        logger.info("[Consumer Group File Storage] Found {} groups, processing in {} batches of {} with {} threads",
                    totalGroups, totalBatches, CONSUMER_GROUP_BATCH_SIZE, CONSUMER_GROUP_THREAD_POOL);
        appendToConsole("[Consumer Group File Storage] Processing " + totalGroups + " groups in " + totalBatches + " batches...");

        // Create batches of group IDs for efficient API calls
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < allGroupIds.size(); i += CONSUMER_GROUP_BATCH_SIZE) {
            batches.add(allGroupIds.subList(i, Math.min(i + CONSUMER_GROUP_BATCH_SIZE, allGroupIds.size())));
        }

        // Use ExecutorService with fixed thread pool for parallel processing
        ExecutorService executorService = Executors.newFixedThreadPool(CONSUMER_GROUP_THREAD_POOL);
        CountDownLatch latch = new CountDownLatch(batches.size());
        long startTime = System.currentTimeMillis();

        for (List<String> batch : batches) {
            executorService.submit(() -> {
                int retries = 0;
                int maxRetries = 3;
                boolean success = false;

                while (retries < maxRetries && !success) {
                    try {
                        processConsumerGroupBatch(batch, client, topicToGroupsMap, topics);
                        success = true;

                        int processed = processedCount.addAndGet(batch.size());
                        int percent = (processed * 100) / totalGroups;

                        // Fast progress updates for large batches
                        if (processed % 50 == 0 || processed == totalGroups || percent % 10 == 0) {
                            long elapsed = System.currentTimeMillis() - startTime;
                            double rate = processed / (elapsed / 1000.0);
                            logger.info("[Consumer Group File Storage] Progress: {}/{} ({}%) - {:.1f} groups/sec",
                                      processed, totalGroups, percent, rate);
                            if (processed % 100 == 0 || processed == totalGroups) {
                                appendToConsole("[Consumer Group File Storage] " + processed + "/" + totalGroups +
                                               " (" + percent + "%) - " + String.format("%.1f", rate) + " groups/sec");
                            }
                        }
                    } catch (Exception e) {
                        retries++;
                        if (retries < maxRetries) {
                            logger.warn("Error processing batch of {} groups (attempt {}/{}), retrying...",
                                      batch.size(), retries, maxRetries);
                            try {
                                Thread.sleep(1000); // Wait 1 second before retry
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        } else {
                            errorCount.addAndGet(batch.size());
                            String errorMsg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
                            if (e.getCause() != null) {
                                errorMsg += " (caused by: " + (e.getCause().getMessage() != null ? e.getCause().getMessage() : e.getCause().getClass().getSimpleName()) + ")";
                            }
                            logger.warn("Error processing batch of {} groups after {} attempts: {}",
                                      batch.size(), maxRetries, errorMsg, e);
                        }
                    }
                }
                latch.countDown();
            });
        }

        // Wait for all tasks to complete with 5 minute timeout
        try {
            if (!latch.await(5, TimeUnit.MINUTES)) {
                logger.warn("[Consumer Group File Storage] Timeout reached - some batches did not complete within 5 minutes");
                appendToConsole("[Consumer Group File Storage] Warning: Timeout - some operations incomplete");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("[Consumer Group File Storage] Interrupted while waiting for batches");
        } finally {
            executorService.shutdownNow();
        }

        long totalTime = System.currentTimeMillis() - startTime;
        int cachedTopics = topicToGroupsMap.size();
        double avgRate = totalGroups / (totalTime / 1000.0);

        logger.info("[Consumer Group File Storage] Completed in {}ms: {} topics cached, {} errors, {:.1f} groups/sec",
                    totalTime, cachedTopics, errorCount.get(), avgRate);
        appendToConsole("[Consumer Group File Storage] Completed in " + (totalTime/1000) + "s: " +
                       cachedTopics + " topics cached (" + errorCount.get() + " errors, " +
                       String.format("%.1f", avgRate) + " groups/sec)");

        return topicToGroupsMap;
    }

    /**
     * Processes a batch of consumer groups (10 at a time) - more efficient API usage.
     * Thread-safe method for parallel execution.
     *
     * @param filterTopics Optional list of topics to filter by (null for all topics)
     */
    private void processConsumerGroupBatch(List<String> groupIds,
                                            org.apache.kafka.clients.admin.AdminClient client,
                                            Map<String, List<ConsumerGroupInfo>> topicToGroupsMap,
                                            List<String> filterTopics) throws Exception {
        // Describe multiple groups in a single API call - much more efficient!
        java.util.Map<String, org.apache.kafka.clients.admin.ConsumerGroupDescription> descriptions =
            client.describeConsumerGroups(groupIds).all().get(60, java.util.concurrent.TimeUnit.SECONDS);

        for (String groupId : groupIds) {
            org.apache.kafka.clients.admin.ConsumerGroupDescription desc = descriptions.get(groupId);
            if (desc == null || desc.members().isEmpty()) continue;

            // Collect all topics consumed by this group
            Set<String> topicsInGroup = new HashSet<>();
            for (org.apache.kafka.clients.admin.MemberDescription member : desc.members()) {
                for (org.apache.kafka.common.TopicPartition tp : member.assignment().topicPartitions()) {
                    topicsInGroup.add(tp.topic());
                }
            }

            // Create ConsumerGroupInfo for each topic consumed by this group
            for (String topic : topicsInGroup) {
                // Apply topic filter if provided
                if (filterTopics != null && !filterTopics.contains(topic)) continue;

                ConsumerGroupInfo info = new ConsumerGroupInfo(groupId, topic);

                // Add all members that consume this topic with their assigned partitions
                for (org.apache.kafka.clients.admin.MemberDescription m : desc.members()) {
                    ConsumerMemberInfo mi = new ConsumerMemberInfo(
                        m.consumerId(), m.clientId(), m.host()
                    );
                    for (org.apache.kafka.common.TopicPartition t : m.assignment().topicPartitions()) {
                        if (t.topic().equals(topic)) {
                            mi.assignedPartitions.add(t.partition() + "");
                        }
                    }
                    if (!mi.assignedPartitions.isEmpty()) {
                        info.members.add(mi);
                    }
                }

                // Note: Lag is NOT fetched here during caching - it will be fetched at runtime
                // Add to list - supports multiple groups per topic
                topicToGroupsMap.computeIfAbsent(topic, k -> new ArrayList<>()).add(info);
            }
        }
    }

    private void fetchConsumerLag(ConsumerGroupInfo info,
                                  org.apache.kafka.clients.admin.AdminClient client) throws Exception {
        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets =
            client.listConsumerGroupOffsets(info.groupId)
                .partitionsToOffsetAndMetadata().get(20, java.util.concurrent.TimeUnit.SECONDS);

        java.util.Set<org.apache.kafka.common.TopicPartition> tps = offsets.keySet().stream()
            .filter(tp -> tp.topic().equals(info.topic))
            .collect(java.util.stream.Collectors.toSet());

        if (tps.isEmpty()) return;

        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
            client.listOffsets(
                tps.stream().collect(java.util.stream.Collectors.toMap(
                    tp -> tp,
                    tp -> org.apache.kafka.clients.admin.OffsetSpec.forTimestamp(java.lang.Long.MAX_VALUE)
                ))
            ).all().get(20, java.util.concurrent.TimeUnit.SECONDS);

        long totalLag = 0;
        for (org.apache.kafka.common.TopicPartition tp : tps) {
            long committed = offsets.get(tp).offset();
            long end = endOffsets.get(tp).offset();
            long lag = Math.max(0, end - committed);
            String partitionKey = tp.partition() + "";
            info.partitionLag.put(partitionKey, lag);
            info.committedOffset.put(partitionKey, committed);
            info.endOffset.put(partitionKey, end);
            totalLag += lag;
        }
        info.totalLag = totalLag;
    }

    private void displayConsumerGroupInfo(ConsumerGroupInfo info) {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Consumer Group: ").append(info.groupId).append(" ===\n");
        sb.append("Topic: ").append(info.topic).append("\n");
        sb.append("State: ").append(info.members.isEmpty() ? "Empty (no active members)" : "Active").append("\n\n");

        sb.append("Members (").append(info.members.size()).append("):\n");
        for (ConsumerMemberInfo m : info.members) {
            sb.append(String.format("  - %s (client: %s, host: %s)\n", m.memberId, m.clientId, m.host));
            sb.append("    Partitions: ").append(String.join(", ", m.assignedPartitions)).append("\n");
        }

        if (info.members.isEmpty()) {
            sb.append("  (No active members - group may be inactive)\n");
        }

        cgDetailsArea.setText(sb.toString());
    }

    private ConsumerGroupInfo getCachedConsumerGroupInfo(String podName, String topicName) {
        if (podName == null || topicName == null) return null;
        // Read from file - no in-memory cache
        List<ConsumerGroupInfo> infoList = readGroupsForTopicFromFile(podName, topicName);
        if (infoList == null || infoList.isEmpty()) return null;
        // Return first group from list
        return infoList.get(0);
    }

    private void cacheConsumerGroupInfo(String podName, String topicName, ConsumerGroupInfo info) {
        // Write to file - no in-memory cache
        // Read existing data from file
        java.io.File file = cgCacheFile(podName);
        Map<String, List<ConsumerGroupInfo>> data = new HashMap<>();
        if (file.exists()) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
                com.fasterxml.jackson.databind.JsonNode root = om.readTree(file);
                // Parse all topics from file
                for (java.util.Iterator<String> it = root.fieldNames(); it.hasNext(); ) {
                    String key = it.next();
                    if ("_timestamp".equals(key)) continue;
                    com.fasterxml.jackson.databind.JsonNode topicNode = root.get(key);
                    List<ConsumerGroupInfo> groups = new ArrayList<>();
                    if (topicNode != null && topicNode.isArray()) {
                        for (com.fasterxml.jackson.databind.JsonNode gNode : topicNode) {
                            ConsumerGroupInfo cg = new ConsumerGroupInfo(
                                gNode.path("groupId").asText(), gNode.path("topic").asText());
                            groups.add(cg);
                        }
                    }
                    data.put(key, groups);
                }
            } catch (Exception e) {
                logger.warn("[CG File Storage] Failed to read existing cache for update: {}", e.getMessage());
            }
        }
        // Add/update this topic's data
        data.computeIfAbsent(topicName, k -> new ArrayList<>()).add(info);
        // Write back to file
        writeCgCacheToFile(podName, data);
    }

    private String formatAge(long timestamp) {
        long minutes = (System.currentTimeMillis() - timestamp) / 60000;
        if (minutes < 1) return "just now";
        if (minutes < 60) return minutes + "m ago";
        return (minutes / 60) + "h ago";
    }

    @FXML
    private void handleLoadConsumerGroupsForLagTopic() {
        // Prevent duplicate loading
        if (consumerGroupsLoading) {
            cgLagStatusLabel.setText("⏳ Already loading groups...");
            return;
        }

        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            cgLagStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = cgLagTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            cgLagStatusLabel.setText("⚠ Select a topic first");
            return;
        }
        topicName = topicName.trim();

        final String finalTopicName = topicName;
        // Capture current pod name before starting thread
        final String podName = currentPodName;
        consumerGroupsLoading = true;
        cgLagStatusLabel.setText("⏳ Loading groups...");
        appendToConsole("[Consumer Lag] Loading groups for topic: " + finalTopicName);

        new Thread(() -> {
            try {
                java.util.Set<String> groupIds = new java.util.HashSet<>();
                boolean wasFromFile = false;

                // Step 1: Try to read from file first
                List<ConsumerGroupInfo> groupsFromFile = readGroupsForTopicFromFile(podName, finalTopicName);

                if (groupsFromFile != null && !groupsFromFile.isEmpty()) {
                    // File cache hit
                    wasFromFile = true;
                    for (ConsumerGroupInfo info : groupsFromFile) {
                        groupIds.add(info.groupId);
                    }
                    appendToConsole("[Consumer Lag] Loaded " + groupIds.size() + " group(s) from file");
                } else {
                    // Step 2: File miss - fetch from Kafka and save to file
                    appendToConsole("[Consumer Lag] File cache miss - fetching from Kafka...");
                    Map<String, List<ConsumerGroupInfo>> allGroups = fetchAllConsumerGroupsForPodAndCacheToFile(podName, client);

                    List<ConsumerGroupInfo> topicGroups = allGroups.get(finalTopicName);
                    if (topicGroups != null) {
                        for (ConsumerGroupInfo info : topicGroups) {
                            groupIds.add(info.groupId);
                        }
                    }
                    appendToConsole("[Consumer Lag] Fetched " + groupIds.size() + " group(s) from Kafka and saved to file");
                }

                final java.util.List<String> finalGroupIds = new java.util.ArrayList<>(groupIds);
                final boolean finalWasFromFile = wasFromFile;
                Platform.runLater(() -> {
                    cgLagConsumerGroupComboBox.getItems().clear();
                    if (!finalGroupIds.isEmpty()) {
                        cgLagConsumerGroupComboBox.getItems().addAll(finalGroupIds);
                        cgLagConsumerGroupComboBox.setValue(finalGroupIds.get(0));
                        cgLagStatusLabel.setText("✓ Found " + finalGroupIds.size() + " group(s)" + (finalWasFromFile ? " (from file)" : ""));
                        appendToConsole("[Consumer Lag] Found " + finalGroupIds.size() + " group(s) for topic " + finalTopicName);
                    } else {
                        cgLagStatusLabel.setText("⚠ No consumer group found");
                        appendToConsole("[Consumer Lag] No consumer groups found for topic " + finalTopicName);
                    }
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    cgLagStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[Consumer Lag] ERROR loading groups: " + e.getMessage());
                });
            } finally {
                consumerGroupsLoading = false;
            }
        }).start();
    }

    @FXML
    private void handleRefreshConsumerLag() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            cgLagStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = cgLagTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            cgLagStatusLabel.setText("⚠ Select a topic");
            return;
        }
        topicName = topicName.trim();

        String selectedGroupId = cgLagConsumerGroupComboBox.getValue();

        final String finalTopicName = topicName;
        final String finalGroupId = selectedGroupId;
        // Capture current pod name before starting thread
        final String podName = currentPodName;
        cgLagStatusLabel.setText("Starting live lag...");
        appendToConsole("[Consumer Lag] Starting live lag for topic: " + finalTopicName +
                         (finalGroupId != null ? ", group: " + finalGroupId : ""));

        stopLagScheduler();
        initLagChart();

        Platform.runLater(() -> {
            if (stopLagButton != null) stopLagButton.setDisable(false);
            cgLagLiveLabel.setText("● Live");
        });

        lagScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lag-scheduler");
            t.setDaemon(true);
            return t;
        });
        lagScheduler.scheduleAtFixedRate(() -> {
            try {
                org.apache.kafka.clients.admin.AdminClient schedClient = getCurrentAdminClient();
                if (schedClient == null) {
                    Platform.runLater(() -> cgLagStatusLabel.setText("⚠ Not connected"));
                    stopLagScheduler();
                    return;
                }
                ConsumerGroupInfo info;
                if (finalGroupId != null && !finalGroupId.trim().isEmpty()) {
                    info = describeSpecificConsumerGroup(finalGroupId.trim(), finalTopicName, schedClient);
                } else {
                    info = describeConsumerGroupForTopic(finalTopicName, schedClient, podName);
                }
                if (info != null) {
                    fetchConsumerLag(info, schedClient);
                    final ConsumerGroupInfo finalInfo = info;
                    Platform.runLater(() -> {
                        displayConsumerLagTable(finalInfo);
                        cgTotalLagLabel.setText("Total Lag: " + finalInfo.totalLag + " messages");
                        String now = java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
                        cgLagLastUpdatedLabel.setText("Last updated: " + now);
                        cgLagStatusLabel.setText("✓ Live");
                        appendLagChartPoint(now, finalInfo.totalLag);
                        appendToConsole("[Consumer Lag] Total lag: " + finalInfo.totalLag);
                    });
                } else {
                    Platform.runLater(() -> {
                        cgLagStatusLabel.setText("⚠ No consumer group found");
                        cgLagTable.getItems().clear();
                        cgTotalLagLabel.setText("Total Lag: -");
                    });
                }
            } catch (Exception e) {
                Platform.runLater(() -> {
                    cgLagStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[Consumer Lag] ERROR: " + e.getMessage());
                });
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    @FXML
    private void handleStopLag() {
        stopLagScheduler();
        appendToConsole("[Consumer Lag] Live lag stopped by user");
    }

    private void stopLagScheduler() {
        if (lagScheduler != null && !lagScheduler.isShutdown()) {
            lagScheduler.shutdownNow();
            lagScheduler = null;
        }
        Platform.runLater(() -> {
            if (stopLagButton != null) stopLagButton.setDisable(true);
            cgLagLiveLabel.setText("");
        });
    }

    private void initLagChart() {
        Platform.runLater(() -> {
            if (cgLagTrendChart == null) return;
            cgLagTrendChart.getData().clear();
            lagTrendSeries = new XYChart.Series<>();
            lagTrendSeries.setName("Total Lag");
            cgLagTrendChart.getData().add(lagTrendSeries);
        });
    }

    private void appendLagChartPoint(String timeLabel, long totalLag) {
        if (lagTrendSeries == null) return;
        lagTrendSeries.getData().add(new XYChart.Data<>(timeLabel, totalLag));
        if (lagTrendSeries.getData().size() > LAG_CHART_MAX_POINTS) {
            lagTrendSeries.getData().remove(0);
        }
    }

    private void displayConsumerLagTable(ConsumerGroupInfo info) {
        @SuppressWarnings("unchecked")
        javafx.scene.control.TableView<javafx.collections.ObservableList<String>> typedTable =
            (javafx.scene.control.TableView<javafx.collections.ObservableList<String>>) (javafx.scene.control.TableView<?>) cgLagTable;
        typedTable.getItems().clear();

        // Set up cell value factories for all columns
        @SuppressWarnings("unchecked")
        javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String> partitionCol =
            (javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String>) typedTable.getColumns().get(0);
        partitionCol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().size() > 0 ? cd.getValue().get(0) : ""));

        @SuppressWarnings("unchecked")
        javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String> committedCol =
            (javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String>) typedTable.getColumns().get(1);
        committedCol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().size() > 1 ? cd.getValue().get(1) : ""));

        @SuppressWarnings("unchecked")
        javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String> endOffsetCol =
            (javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String>) typedTable.getColumns().get(2);
        endOffsetCol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().size() > 2 ? cd.getValue().get(2) : ""));

        @SuppressWarnings("unchecked")
        javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String> lagCol =
            (javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String>) typedTable.getColumns().get(3);
        lagCol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().size() > 3 ? cd.getValue().get(3) : ""));

        @SuppressWarnings("unchecked")
        javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String> memberIdCol =
            (javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String>) typedTable.getColumns().get(5);
        memberIdCol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().size() > 5 ? cd.getValue().get(5) : ""));

        @SuppressWarnings("unchecked")
        javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String> clientIdCol =
            (javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String>) typedTable.getColumns().get(6);
        clientIdCol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().size() > 6 ? cd.getValue().get(6) : ""));

        // Wire lag bar column cell factory once
        if (cgLagBarColumn != null) {
            @SuppressWarnings("unchecked")
            javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String> barCol =
                (javafx.scene.control.TableColumn<javafx.collections.ObservableList<String>, String>) (javafx.scene.control.TableColumn<?, ?>) cgLagBarColumn;
            barCol.setCellValueFactory(cd -> new javafx.beans.property.SimpleStringProperty(cd.getValue().size() > 3 ? cd.getValue().get(3) : "0"));
            barCol.setCellFactory(col -> new javafx.scene.control.TableCell<javafx.collections.ObservableList<String>, String>() {
                private final javafx.scene.control.ProgressBar bar = new javafx.scene.control.ProgressBar(0);
                {
                    bar.setMaxWidth(Double.MAX_VALUE);
                }
                @Override
                protected void updateItem(String lagStr, boolean empty) {
                    super.updateItem(lagStr, empty);
                    if (empty || lagStr == null) { setGraphic(null); return; }
                    try {
                        long lag = Long.parseLong(lagStr);
                        // Find max lag in table for relative bar scaling
                        long maxLag = typedTable.getItems().stream()
                            .filter(r -> r.size() > 3)
                            .mapToLong(r -> { try { return Long.parseLong(r.get(3)); } catch (Exception e) { return 0; } })
                            .max().orElse(1);
                        double progress = maxLag > 0 ? (double) lag / maxLag : 0;
                        bar.setProgress(progress);
                        String color = lag == 0 ? "#95a5a6" : lag <= 100 ? "#27ae60" : lag <= 1000 ? "#e67e22" : "#e74c3c";
                        bar.setStyle("-fx-accent: " + color + ";");
                        String label = lag == 0 ? "OK" : lag <= 100 ? "LOW" : lag <= 1000 ? "MEDIUM" : "HIGH";
                        setText(label);
                        setGraphic(bar);
                    } catch (Exception e) { setGraphic(null); }
                }
            });
        }

        long maxLag = info.partitionLag.values().stream().mapToLong(v -> v).max().orElse(1);

        // Get all partitions for the topic, not just those with lag data
        List<org.apache.kafka.common.TopicPartition> partitions;
        try {
            org.apache.kafka.clients.admin.AdminClient adminClient = getCurrentAdminClient();
            if (adminClient != null) {
                org.apache.kafka.clients.admin.TopicDescription topicDesc = adminClient.describeTopics(
                    java.util.Collections.singletonList(info.topic))
                    .allTopicNames().get(5, java.util.concurrent.TimeUnit.SECONDS).get(info.topic);
                
                if (topicDesc != null) {
                    partitions = topicDesc.partitions().stream()
                        .map(p -> new org.apache.kafka.common.TopicPartition(info.topic, p.partition()))
                        .sorted(java.util.Comparator.comparingInt(org.apache.kafka.common.TopicPartition::partition))
                        .collect(java.util.stream.Collectors.toList());
                } else {
                    partitions = info.partitionLag.keySet().stream()
                        .map(p -> new org.apache.kafka.common.TopicPartition(info.topic, Integer.parseInt(p)))
                        .sorted(java.util.Comparator.comparingInt(org.apache.kafka.common.TopicPartition::partition))
                        .collect(java.util.stream.Collectors.toList());
                }
            } else {
                partitions = info.partitionLag.keySet().stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(info.topic, Integer.parseInt(p)))
                    .sorted(java.util.Comparator.comparingInt(org.apache.kafka.common.TopicPartition::partition))
                    .collect(java.util.stream.Collectors.toList());
            }
        } catch (Exception e) {
            // Fallback to partitions with lag data if we can't get topic info
            partitions = info.partitionLag.keySet().stream()
                .map(p -> new org.apache.kafka.common.TopicPartition(info.topic, Integer.parseInt(p)))
                .sorted(java.util.Comparator.comparingInt(org.apache.kafka.common.TopicPartition::partition))
                .collect(java.util.stream.Collectors.toList());
        }

        for (org.apache.kafka.common.TopicPartition tp : partitions) {
            String partition = tp.partition() + "";
            Long lag = info.partitionLag.get(partition);
            Long committed = info.committedOffset.get(partition);
            Long end = info.endOffset.get(partition);

            // If we don't have offset data for this partition, try to fetch it
            if (lag == null || committed == null || end == null) {
                try {
                    org.apache.kafka.clients.admin.AdminClient adminClient = getCurrentAdminClient();
                    if (adminClient != null) {
                        // Fetch end offset if missing
                        if (end == null) {
                            java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                                adminClient.listOffsets(
                                    java.util.Collections.singletonMap(
                                        tp,
                                        org.apache.kafka.clients.admin.OffsetSpec.forTimestamp(java.lang.Long.MAX_VALUE)
                                    )
                                ).all().get(3, java.util.concurrent.TimeUnit.SECONDS);
                            org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo endOffsetInfo = endOffsets.get(tp);
                            if (endOffsetInfo != null) {
                                end = endOffsetInfo.offset();
                            }
                        }
                        
                        // If we have end offset but no lag, calculate lag as end offset (no committed offset)
                        if (lag == null && end != null) {
                            lag = end; // Full topic lag when no committed offset
                        }
                        
                        // If we still don't have end offset, set lag to 0
                        if (lag == null) {
                            lag = 0L;
                        }
                    }
                } catch (Exception e) {
                    // If we can't fetch data, set defaults
                    if (lag == null) lag = 0L;
                }
            }

            String memberId = "N/A";
            String clientId = "N/A";
            for (ConsumerMemberInfo m : info.members) {
                if (m.assignedPartitions.contains(partition)) {
                    memberId = m.memberId;
                    clientId = m.clientId;
                    break;
                }
            }

            javafx.collections.ObservableList<String> row = javafx.collections.FXCollections.observableArrayList(
                partition,
                committed != null ? committed.toString() : "-",
                end != null ? end.toString() : "-",
                lag != null ? lag.toString() : "0",
                "",
                memberId,
                clientId
            );
            typedTable.getItems().add(row);
        }
    }

    @FXML
    private void handleLoadConsumerGroupsForReset() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            cgResetStatusLabel.setText("⚠ Not connected");
            return;
        }

        String topicName = cgResetTopicComboBox.getValue();
        if (topicName == null || topicName.trim().isEmpty()) {
            cgResetStatusLabel.setText("⚠ Select a topic first");
            return;
        }
        topicName = topicName.trim();

        final String finalTopicName = topicName;
        // Capture current pod name before starting thread
        final String podName = currentPodName;
        cgResetStatusLabel.setText("⏳ Loading groups...");
        appendToConsole("[Reset Offsets] Loading groups for topic: " + finalTopicName);

        new Thread(() -> {
            try {
                java.util.Set<String> groupIds = new java.util.HashSet<>();
                boolean wasFromFile = false;

                // Step 1: Try to read from file first
                List<ConsumerGroupInfo> groupsFromFile = readGroupsForTopicFromFile(podName, finalTopicName);

                if (groupsFromFile != null && !groupsFromFile.isEmpty()) {
                    // File cache hit
                    wasFromFile = true;
                    for (ConsumerGroupInfo info : groupsFromFile) {
                        groupIds.add(info.groupId);
                    }
                    appendToConsole("[Reset Offsets] Loaded " + groupIds.size() + " group(s) from file");
                } else {
                    // Step 2: File miss - fetch from Kafka and save to file
                    appendToConsole("[Reset Offsets] File cache miss - fetching from Kafka...");
                    Map<String, List<ConsumerGroupInfo>> allGroups = fetchAllConsumerGroupsForPodAndCacheToFile(podName, client);

                    List<ConsumerGroupInfo> topicGroups = allGroups.get(finalTopicName);
                    if (topicGroups != null) {
                        for (ConsumerGroupInfo info : topicGroups) {
                            groupIds.add(info.groupId);
                        }
                    }
                    appendToConsole("[Reset Offsets] Fetched " + groupIds.size() + " group(s) from Kafka and saved to file");
                }

                final java.util.List<String> finalGroupIds = new java.util.ArrayList<>(groupIds);
                final boolean finalWasFromFile = wasFromFile;
                Platform.runLater(() -> {
                    cgResetGroupIdComboBox.getItems().clear();
                    if (!finalGroupIds.isEmpty()) {
                        cgResetGroupIdComboBox.getItems().addAll(finalGroupIds);
                        cgResetGroupIdComboBox.setValue(finalGroupIds.get(0));
                        cgResetStatusLabel.setText("✓ Found " + finalGroupIds.size() + " group(s)" + (finalWasFromFile ? " (from file)" : ""));
                        appendToConsole("[Reset Offsets] Found " + finalGroupIds.size() + " group(s) for topic " + finalTopicName);
                    } else {
                        cgResetStatusLabel.setText("⚠ No consumer group found");
                        appendToConsole("[Reset Offsets] No consumer groups found for topic " + finalTopicName);
                    }
                });
            } catch (Exception e) {
                Platform.runLater(() -> {
                    cgResetStatusLabel.setText("⚠ Error: " + e.getMessage());
                    appendToConsole("[Reset Offsets] ERROR loading groups: " + e.getMessage());
                });
            }
        }).start();
    }

    @FXML
    private void handleResetOffsets() {
        org.apache.kafka.clients.admin.AdminClient client = getCurrentAdminClient();
        if (client == null) {
            cgResetStatusLabel.setText("⚠ Not connected");
            return;
        }

        String groupId = cgResetGroupIdComboBox.getValue();
        if (groupId == null || groupId.trim().isEmpty()) {
            cgResetStatusLabel.setText("⚠ Group ID required");
            return;
        }
        groupId = groupId.trim();

        String topicName = cgResetTopicComboBox.getValue();
        String strategy = cgResetStrategyCombo.getValue();

        if (topicName == null || topicName.trim().isEmpty()) {
            cgResetStatusLabel.setText("⚠ Topic required");
            return;
        }
        if (strategy == null) {
            cgResetStatusLabel.setText("⚠ Strategy required");
            return;
        }

        topicName = topicName.trim();

        // Confirmation dialog
        final String finalGroupId = groupId;
        final String finalTopicName = topicName;
        final String finalStrategy = strategy;

        // Build confirmation message
        StringBuilder confirmMsg = new StringBuilder();
        confirmMsg.append("Strategy: ").append(finalStrategy).append("\nTopic: ").append(finalTopicName);
        if ("to-datetime".equals(finalStrategy)) {
            java.time.LocalDate date = cgResetDatePicker.getValue();
            int hour = cgResetHourSpinner.getValue();
            int minute = cgResetMinuteSpinner.getValue();
            confirmMsg.append("\nDate/Time: ").append(date).append(" ")
                      .append(String.format("%02d:%02d", hour, minute));
        } else if ("shift-by".equals(finalStrategy)) {
            String shiftValue = cgResetShiftField.getText().trim();
            confirmMsg.append("\nShift: ").append(shiftValue).append(" offsets");
        }

        javafx.scene.control.Alert confirm = new javafx.scene.control.Alert(javafx.scene.control.Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Reset Offsets");
        confirm.setHeaderText("Reset offsets for group: " + finalGroupId + "?");
        confirm.setContentText(confirmMsg.toString());

        confirm.showAndWait().ifPresent(response -> {
            if (response == javafx.scene.control.ButtonType.OK) {
                cgResetStatusLabel.setText("⏳ Resetting...");
                appendToConsole("[Reset Offsets] Group: " + finalGroupId + ", Strategy: " + finalStrategy);

                new Thread(() -> {
                    try {
                        // Get topic partitions
                        java.util.Map<String, org.apache.kafka.clients.admin.TopicDescription> topics =
                            client.describeTopics(java.util.Collections.singleton(finalTopicName))
                                .allTopicNames().get(20, java.util.concurrent.TimeUnit.SECONDS);

                        org.apache.kafka.clients.admin.TopicDescription desc = topics.get(finalTopicName);
                        if (desc == null) {
                            Platform.runLater(() -> cgResetStatusLabel.setText("⚠ Topic not found"));
                            return;
                        }

                        java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> newOffsets =
                            new java.util.HashMap<>();

                        for (org.apache.kafka.common.TopicPartitionInfo partition : desc.partitions()) {
                            org.apache.kafka.common.TopicPartition tp =
                                new org.apache.kafka.common.TopicPartition(finalTopicName, partition.partition());

                            long newOffset;
                            switch (finalStrategy) {
                                case "earliest":
                                    // Get earliest offset
                                    newOffset = 0;
                                    break;
                                case "latest":
                                    // Get latest offset - use listOffsets
                                    java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> latestInfo =
                                        client.listOffsets(java.util.Collections.singletonMap(tp,
                                            org.apache.kafka.clients.admin.OffsetSpec.forTimestamp(java.lang.System.currentTimeMillis())))
                                            .all().get(10, java.util.concurrent.TimeUnit.SECONDS);
                                    newOffset = latestInfo.get(tp).offset();
                                    break;
                                case "to-datetime":
                                    // Build ISO datetime from DatePicker and spinners
                                    java.time.LocalDate date = cgResetDatePicker.getValue();
                                    int hour = cgResetHourSpinner.getValue();
                                    int minute = cgResetMinuteSpinner.getValue();
                                    java.time.LocalDateTime dateTime = java.time.LocalDateTime.of(date, java.time.LocalTime.of(hour, minute));
                                    java.time.Instant instant = dateTime.atZone(java.time.ZoneId.systemDefault()).toInstant();
                                    java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo> tsInfo =
                                        client.listOffsets(java.util.Collections.singletonMap(tp,
                                            org.apache.kafka.clients.admin.OffsetSpec.forTimestamp(instant.toEpochMilli())))
                                            .all().get(10, java.util.concurrent.TimeUnit.SECONDS);
                                    newOffset = tsInfo.get(tp).offset();
                                    break;
                                case "shift-by":
                                    // Get current position and shift
                                    java.util.Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> currentOffsets =
                                        client.listConsumerGroupOffsets(finalGroupId)
                                            .partitionsToOffsetAndMetadata().get(20, java.util.concurrent.TimeUnit.SECONDS);
                                    long shift = Long.parseLong(cgResetShiftField.getText().trim());
                                    newOffset = currentOffsets.getOrDefault(tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(0)).offset() + shift;
                                    break;
                                default:
                                    throw new UnsupportedOperationException("Unknown strategy: " + finalStrategy);
                            }

                            newOffsets.put(tp, new org.apache.kafka.clients.consumer.OffsetAndMetadata(newOffset));
                        }

                        client.alterConsumerGroupOffsets(finalGroupId, newOffsets).all().get(20, java.util.concurrent.TimeUnit.SECONDS);

                        Platform.runLater(() -> {
                            cgResetStatusLabel.setText("✓ Offsets reset");
                            appendToConsole("[Reset Offsets] Successfully reset for group: " + finalGroupId);
                        });
                    } catch (Exception e) {
                        Platform.runLater(() -> {
                            cgResetStatusLabel.setText("⚠ Error: " + e.getMessage());
                            appendToConsole("[Reset Offsets] ERROR: " + e.getMessage());
                        });
                    }
                }).start();
            }
        });
    }

    
    
    
    // ========== EXPORT HANDLERS ==========

    @FXML
    private void handleExportTopic() {
        if (partitionOffsetTable.getItems().isEmpty()) {
            appendToConsole("[Export] No topic data to export. Please describe a topic first.");
            return;
        }

        FileChooser fileChooser = exportService.createCSVFileChooser("partition-offsets");
        File file = fileChooser.showSaveDialog(getStage());
        
        if (file != null) {
            // Run export in background thread to avoid blocking UI
            CompletableFuture.runAsync(() -> {
                try {
                    exportService.exportTableViewToCSV(partitionOffsetTable, file.getAbsolutePath());
                    Platform.runLater(() -> 
                        appendToConsole("[Export] Partition offsets exported to: " + file.getName()));
                } catch (Exception e) {
                    Platform.runLater(() -> {
                        appendToConsole("[Export] Error exporting partition offsets: " + e.getMessage());
                        logger.error("Error exporting partition offsets", e);
                    });
                }
            }).whenComplete((result, throwable) -> {
                // Ensure thread cleanup
                if (throwable != null) {
                    logger.error("Export thread completed with error", throwable);
                }
            });
        }
    }

    @FXML
    private void handleExportTopicDetails() {
        String topicDetails = topicDetailsArea.getText();
        if (topicDetails.isEmpty() || topicDetails.contains("Topic details will appear here")) {
            appendToConsole("[Export] No topic details to export. Please describe a topic first.");
            return;
        }

        FileChooser fileChooser = exportService.createTextFileChooser("topic-details");
        File file = fileChooser.showSaveDialog(getStage());
        
        if (file != null) {
            // Run export in background thread to avoid blocking UI
            CompletableFuture.runAsync(() -> {
                try {
                    // Export topic details as text content with proper encoding
                    byte[] topicBytes = topicDetails.getBytes(StandardCharsets.UTF_8);
                    Files.write(Paths.get(file.getAbsolutePath()), topicBytes);
                    Platform.runLater(() -> 
                        appendToConsole("[Export] Topic details exported to: " + file.getName()));
                } catch (Exception e) {
                    Platform.runLater(() -> {
                        appendToConsole("[Export] Error exporting topic details: " + e.getMessage());
                        logger.error("Error exporting topic details", e);
                    });
                }
            }).whenComplete((result, throwable) -> {
                // Ensure thread cleanup
                if (throwable != null) {
                    logger.error("Export thread completed with error", throwable);
                }
            });
        }
    }

    @FXML
    private void handleExportGroup() {
        String groupDetails = cgDetailsArea.getText();
        if (groupDetails.isEmpty() || groupDetails.contains("Consumer group details will appear here")) {
            appendToConsole("[Export] No group data to export. Please describe a consumer group first.");
            return;
        }

        FileChooser fileChooser = exportService.createTextFileChooser("consumer-group-details");
        File file = fileChooser.showSaveDialog(getStage());
        
        if (file != null) {
            // Run export in background thread to avoid blocking UI
            CompletableFuture.runAsync(() -> {
                try {
                    // Export group details as text content with proper encoding
                    byte[] groupBytes = groupDetails.getBytes(StandardCharsets.UTF_8);
                    Files.write(Paths.get(file.getAbsolutePath()), groupBytes);
                    Platform.runLater(() -> 
                        appendToConsole("[Export] Consumer group details exported to: " + file.getName()));
                } catch (Exception e) {
                    Platform.runLater(() -> {
                        appendToConsole("[Export] Error exporting group details: " + e.getMessage());
                        logger.error("Error exporting group details", e);
                    });
                }
            }).whenComplete((result, throwable) -> {
                // Ensure thread cleanup
                if (throwable != null) {
                    logger.error("Export thread completed with error", throwable);
                }
            });
        }
    }

    @FXML
    private void handleExportLagTable() {
        if (cgLagTable.getItems().isEmpty()) {
            appendToConsole("[Export] No lag data to export. Please load consumer lag data first.");
            return;
        }

        FileChooser fileChooser = exportService.createCSVFileChooser("consumer-lag");
        File file = fileChooser.showSaveDialog(getStage());
        
        if (file != null) {
            // Run export in background thread to avoid blocking UI
            CompletableFuture.runAsync(() -> {
                try {
                    exportService.exportTableViewToCSV(cgLagTable, file.getAbsolutePath());
                    Platform.runLater(() -> 
                        appendToConsole("[Export] Consumer lag table exported to: " + file.getName()));
                } catch (Exception e) {
                    Platform.runLater(() -> {
                        appendToConsole("[Export] Error exporting lag table: " + e.getMessage());
                        logger.error("Error exporting lag table", e);
                    });
                }
            }).whenComplete((result, throwable) -> {
                // Ensure thread cleanup
                if (throwable != null) {
                    logger.error("Export thread completed with error", throwable);
                }
            });
        }
    }

    @FXML
    private void handleExportLagChart() {
        if (cgLagTrendChart.getData().isEmpty()) {
            appendToConsole("[Export] No chart data to export. Please start live lag monitoring first.");
            return;
        }

        FileChooser fileChooser = exportService.createPNGFileChooser("consumer-lag-chart");
        File file = fileChooser.showSaveDialog(getStage());
        
        if (file != null) {
            // Run export in background thread to avoid blocking UI
            CompletableFuture.runAsync(() -> {
                try {
                    exportService.exportChartToPNG(cgLagTrendChart, file.getAbsolutePath());
                    Platform.runLater(() -> 
                        appendToConsole("[Export] Consumer lag chart exported to: " + file.getName()));
                } catch (Exception e) {
                    Platform.runLater(() -> {
                        appendToConsole("[Export] Error exporting lag chart: " + e.getMessage());
                        logger.error("Error exporting lag chart", e);
                    });
                }
            }).whenComplete((result, throwable) -> {
                // Ensure thread cleanup
                if (throwable != null) {
                    logger.error("Export thread completed with error", throwable);
                }
            });
        }
    }

    private Stage getStage() {
        return (Stage) cgLagTable.getScene().getWindow();
    }

}
