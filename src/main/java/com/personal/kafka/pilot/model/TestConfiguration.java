package com.personal.kafka.pilot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TestConfiguration {
    
    private String podNumber;
    
    private String sourceTopicName;
    
    private Integer sourcePartition;
    
    private Long sourceOffset;
    
    private String sourceKeyDeserializerClass;
    
    private String sourceValueDeserializerClass;
    
    private String topicName;

    private Integer targetPartition;

    private int totalMessageCount;
    
    private int threadPoolSize;
    
    private int batchSize;
    
    private String messageTemplate;
    
    private String keySerializerClass;
    
    private String valueSerializerClass;
    
    private String protobufClassName;
    
    private List<FieldConfig> autoIncrementFields;
    
    private List<FieldConfig> rotationFields;
    
    private List<FieldConfig> uuidFields;
    
    private Map<String, String> additionalKafkaProperties;
    
    private int brokerCount;
    
    private String configName;
    
    private String customJarPath;
    
    private String mavenDependency;
    
    private Map<String, String> messageHeaders;

    private String messageKeyField;
    private String messageKeyHardcoded;
    private String messageKeyType; // "none", "field", or "hardcoded"

    // Fields for Search tab (not in original config)
    private String selectedBrokerName;
    private String bootstrapServers;
    private String searchTopic;
    private String searchValueDeserializer;
    private String searchTextFilter;
    private String searchMaxResults;
    private Integer searchPartition;
    private String searchFromTime;
    private String searchToTime;

    // String representations for easy editing
    private String autoIncrementFieldsString;
    private String rotationFieldsString;
    private String uuidFieldsString;
    private String messageHeadersString;

    // Per-pod state storage - each pod has its own complete configuration
    private Map<String, PodState> podStates;

    // Broker configurations stored in config file (replaces brokers.json)
    private List<KafkaBrokerConfig> brokerConfigs;

    public TestConfiguration() {
        this.podNumber = "";
        this.topicName = "";
        this.totalMessageCount = 1;
        this.threadPoolSize = 1;
        this.batchSize = 1;
        this.messageTemplate = "{}";
        this.keySerializerClass = "org.apache.kafka.common.serialization.StringSerializer";
        this.valueSerializerClass = "org.apache.kafka.common.serialization.StringSerializer";
        this.brokerCount = 6;
        this.additionalKafkaProperties = new HashMap<>();
        this.messageHeaders = new HashMap<>();
        this.configName = "Default Configuration";
        this.selectedBrokerName = "";
        this.bootstrapServers = "";
        this.searchTopic = "";
        this.searchValueDeserializer = "";
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }
}
