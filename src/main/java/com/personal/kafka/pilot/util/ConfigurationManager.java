package com.personal.kafka.pilot.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.personal.kafka.pilot.model.KafkaBrokerConfig;
import com.personal.kafka.pilot.model.TestConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigurationManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);
    private static final String MAIN_DIR = "kafka-pilot";
    private static final String CONFIG_DIR = "configs";
    private static final String BROKERS_DIR = "brokers";
    private static final String BROKERS_FILE = "brokers.json";
    private final ObjectMapper objectMapper;
    private final Path configDirectory;

    /** Constructor — stores in working directory (where app is run) */
    public ConfigurationManager() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        String workingDir = System.getProperty("user.dir");
        this.configDirectory = Paths.get(workingDir, MAIN_DIR, CONFIG_DIR);
        initDir();
    }

    /** Constructor with custom path — stores in specified directory */
    public ConfigurationManager(String customPath) {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        this.configDirectory = Paths.get(customPath);
        initDir();
    }

    private void initDir() {
        try {
            Files.createDirectories(configDirectory);
            logger.info("Configuration directory: {}", configDirectory.toAbsolutePath());
            logger.info("Working directory: {}", System.getProperty("user.dir"));
        } catch (IOException e) {
            logger.error("Failed to create configuration directory", e);
        }
    }
    
    public void saveConfiguration(TestConfiguration config, String fileName) throws IOException {
        if (fileName == null || fileName.trim().isEmpty()) {
            fileName = "config_" + System.currentTimeMillis() + ".json";
        }
        
        if (!fileName.endsWith(".json")) {
            fileName += ".json";
        }
        
        Path filePath = configDirectory.resolve(fileName);
        
        logger.info("Saving configuration to: {}", filePath);
        objectMapper.writeValue(filePath.toFile(), config);
        logger.info("Configuration saved successfully");
    }
    
    public TestConfiguration loadConfiguration(String fileName) throws IOException {
        Path filePath = configDirectory.resolve(fileName);
        
        if (!Files.exists(filePath)) {
            logger.error("Configuration file not found: {}", filePath);
            throw new IOException("Configuration file not found: " + fileName);
        }
        
        logger.info("Loading configuration from: {}", filePath);
        TestConfiguration config = objectMapper.readValue(filePath.toFile(), TestConfiguration.class);
        logger.info("Configuration loaded successfully");
        return config;
    }
    
    public List<String> listConfigurations() {
        try {
            if (!Files.exists(configDirectory)) {
                logger.warn("Configuration directory does not exist");
                return new ArrayList<>();
            }
            
            List<String> configs = Files.list(configDirectory)
                .filter(path -> path.toString().endsWith(".json"))
                .map(path -> path.getFileName().toString())
                .sorted()
                .collect(Collectors.toList());
            
            logger.info("Found {} configuration files", configs.size());
            return configs;
            
        } catch (IOException e) {
            logger.error("Error listing configurations", e);
            return new ArrayList<>();
        }
    }
    
    public void deleteConfiguration(String fileName) throws IOException {
        Path filePath = configDirectory.resolve(fileName);
        
        if (!Files.exists(filePath)) {
            logger.error("Configuration file not found: {}", filePath);
            throw new IOException("Configuration file not found: " + fileName);
        }
        
        logger.info("Deleting configuration: {}", filePath);
        Files.delete(filePath);
        logger.info("Configuration deleted successfully");
    }
    
    public Path getConfigDirectory() {
        return configDirectory;
    }

    // ========== BROKER CONFIGURATION METHODS ==========

    public void saveBrokerConfigs(List<KafkaBrokerConfig> brokers) throws IOException {
        String workingDir = System.getProperty("user.dir");
        Path brokersDir = Paths.get(workingDir, MAIN_DIR, BROKERS_DIR);
        Path filePath = brokersDir.resolve(BROKERS_FILE);
        
        // Create broker directory if it doesn't exist
        if (!Files.exists(brokersDir)) {
            Files.createDirectories(brokersDir);
        }
        
        logger.info("Saving broker configurations to: {}", filePath);
        objectMapper.writeValue(filePath.toFile(), brokers);
        logger.info("Broker configurations saved: {} brokers", brokers.size());
    }

    public List<KafkaBrokerConfig> loadBrokerConfigs() {
        // First, try to load from the last saved configuration file
        try {
            Path lastConfigFile = findLastSavedConfiguration();
            if (lastConfigFile != null) {
                logger.info("Loading broker configurations from last saved config: {}", lastConfigFile);
                TestConfiguration config = objectMapper.readValue(lastConfigFile.toFile(), TestConfiguration.class);
                if (config.getBrokerConfigs() != null && !config.getBrokerConfigs().isEmpty()) {
                    logger.info("Loaded {} brokers from configuration file", config.getBrokerConfigs().size());
                    return config.getBrokerConfigs();
                }
            }
        } catch (Exception e) {
            logger.warn("Could not load brokers from config file, falling back to brokers.json", e);
        }

        // Fall back to brokers.json in kafka-pilot/broker directory
        String workingDir = System.getProperty("user.dir");
        Path brokersDir = Paths.get(workingDir, MAIN_DIR, BROKERS_DIR);
        Path filePath = brokersDir.resolve(BROKERS_FILE);

        if (!Files.exists(filePath)) {
            logger.info("Broker configuration file not found, returning empty list");
            return new ArrayList<>();
        }

        try {
            logger.info("Loading broker configurations from: {}", filePath);
            List<KafkaBrokerConfig> brokers = objectMapper.readValue(filePath.toFile(),
                    objectMapper.getTypeFactory().constructCollectionType(List.class, KafkaBrokerConfig.class));
            logger.info("Broker configurations loaded: {} brokers", brokers.size());
            return brokers;
        } catch (IOException e) {
            logger.error("Error loading broker configurations", e);
            return new ArrayList<>();
        }
    }

    /**
     * Loads all pod states from the last saved configuration file into memory.
     * Used on startup to pre-populate podStateMap without touching the UI.
     * @return Map of podName -> PodState, or empty map if none found.
     */
    public java.util.Map<String, com.personal.kafka.pilot.model.PodState> loadPodStatesFromDisk() {
        try {
            Path lastConfigFile = findLastSavedConfiguration();
            if (lastConfigFile != null) {
                TestConfiguration config = objectMapper.readValue(lastConfigFile.toFile(), TestConfiguration.class);
                if (config.getPodStates() != null && !config.getPodStates().isEmpty()) {
                    logger.info("Pre-loaded {} pod states from: {}", config.getPodStates().size(), lastConfigFile.getFileName());
                    return new java.util.HashMap<>(config.getPodStates());
                }
            }
        } catch (Exception e) {
            logger.warn("Could not pre-load pod states from disk", e);
        }
        return new java.util.HashMap<>();
    }

    /**
     * Finds the most recently saved configuration file.
     * @return Path to the last saved config, or null if none found.
     */
    private Path findLastSavedConfiguration() {
        try {
            if (!Files.exists(configDirectory)) {
                return null;
            }

            return Files.list(configDirectory)
                .filter(path -> path.toString().endsWith(".json"))
                .filter(path -> !path.getFileName().toString().equals(BROKERS_FILE))
                .max((p1, p2) -> {
                    try {
                        return Files.getLastModifiedTime(p1).compareTo(Files.getLastModifiedTime(p2));
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .orElse(null);

        } catch (IOException e) {
            logger.error("Error finding last saved configuration", e);
            return null;
        }
    }
}
