package com.personal.kafka.pilot.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.personal.kafka.pilot.model.FieldConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MessageGenerator<T> {
    
    private static final Logger logger = LoggerFactory.getLogger(MessageGenerator.class);
    private final ObjectMapper objectMapper;
    private final String templateJson;
    private final List<FieldConfig> autoIncrementFields;
    private final List<FieldConfig> rotationFields;
    private final List<FieldConfig> uuidFields;
    
    public MessageGenerator(String templateJson, 
                          List<FieldConfig> autoIncrementFields,
                          List<FieldConfig> rotationFields) {
        this(templateJson, autoIncrementFields, rotationFields, null);
    }
    
    public MessageGenerator(String templateJson, 
                          List<FieldConfig> autoIncrementFields,
                          List<FieldConfig> rotationFields,
                          List<FieldConfig> uuidFields) {
        this.objectMapper = new ObjectMapper();
        this.templateJson = templateJson;
        this.autoIncrementFields = autoIncrementFields;
        this.rotationFields = rotationFields;
        this.uuidFields = uuidFields;
    }
    
    public Object[] generateMessageAndJson(int messageIndex) {
        try {
            JsonNode template = objectMapper.readTree(templateJson);
            if (template.isObject()) {
                ObjectNode objectNode = (ObjectNode) template;
                if (autoIncrementFields != null) {
                    for (FieldConfig fieldConfig : autoIncrementFields) {
                        Object value = fieldConfig.getNextValue();
                        setFieldValue(objectNode, fieldConfig.getFieldPath(), value);
                    }
                }
                if (rotationFields != null) {
                    for (FieldConfig fieldConfig : rotationFields) {
                        Object value = fieldConfig.getNextValue();
                        setFieldValue(objectNode, fieldConfig.getFieldPath(), value);
                    }
                }
                if (uuidFields != null) {
                    for (FieldConfig fieldConfig : uuidFields) {
                        Object value = fieldConfig.getNextValue();
                        setFieldValue(objectNode, fieldConfig.getFieldPath(), value);
                    }
                }
                String json = objectMapper.writeValueAsString(objectNode);
                return new Object[]{(T) json, json};
            }
            return new Object[]{(T) templateJson, templateJson};
        } catch (Exception e) {
            logger.error("Error in generateMessageAndJson #{}: {}", messageIndex, e.getMessage(), e);
            return new Object[]{(T) templateJson, templateJson};
        }
    }

    
    protected void setFieldValue(ObjectNode node, String fieldPath, Object value) {
        if (fieldPath == null || fieldPath.isEmpty()) {
            logger.warn("Empty field path provided");
            return;
        }
        
        String[] parts = fieldPath.split("\\.");
        ObjectNode current = node;
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (!current.has(part)) current.putObject(part);
            JsonNode next = current.get(part);
            if (next.isObject()) current = (ObjectNode) next;
            else return;
        }
        String finalField = parts[parts.length - 1];
        if (value instanceof Long)         current.put(finalField, (Long) value);
        else if (value instanceof Integer)  current.put(finalField, (Integer) value);
        else if (value instanceof Double)   current.put(finalField, (Double) value);
        else if (value instanceof String)   current.put(finalField, (String) value);
        else if (value instanceof Boolean)  current.put(finalField, (Boolean) value);
        else if (value != null)             current.put(finalField, value.toString());
    }
}
