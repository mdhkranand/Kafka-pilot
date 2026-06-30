package com.personal.kafka.pilot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * Holds all UI field values for a specific pod/broker.
 * This allows per-pod state management where each pod remembers its own settings.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PodState {

    // Search Tab Fields
    private String searchTopic;
    private String searchTextFilter;
    private boolean searchAllPartitions;
    private String searchSpecificPartitionValue;
    private String sourceOffset;
    private boolean searchNoTimeRange;
    private String searchFromDate;
    private String searchFromHour;
    private String searchFromMinute;
    private String searchToDate;
    private String searchToHour;
    private String searchToMinute;
    private String searchMaxResults;
    private String searchValueDeserializer;
    private String searchFlatbufClassName;
    private boolean decodeAsFlatbuf;
    private String sourceKeyDeserializer;
    // Push Tab Fields
    private String sourceTopic;
    private String sourcePartition;
    private String pushTopic;
    private String targetPartition;
    private String messageCount;
    private String threadPoolSize;
    private String batchSize;
    private String messageTemplate;
    private String keySerializer;
    private String valueSerializer;
    private String protobufClassName;
    private String pushFlatbufClassName;
    private boolean pushFlatbufMode;
    private String messageHeaders;
    private String autoIncrementFields;
    private String rotationFields;
    private String uuidFields;
    private String pushMessageTemplate;
    private String autoIncrementStart;
    private String autoIncrementStep;
    private String rotationValues;
    private String partitionKeyMode; // "all", "specific", "field", "constant"
    private String partitionKeyField;
    private String partitionKeyConstant;

    // Verify Tab Fields
    private String verifyTopic;
    private String verifyTimeout;
    private String verifyPartitionOffsets;

    // Maven Dependency Fields
    private String mavenDependency;

    // Additional properties storage for dynamic fields
    @Builder.Default
    private Map<String, String> additionalProperties = new HashMap<>();
}
