package com.personal.kafka.pilot.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.personal.kafka.pilot.model.FieldConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;

public class ProtobufMessageGenerator<T> extends MessageGenerator<T> {
    
    private static final Logger logger = LoggerFactory.getLogger(ProtobufMessageGenerator.class);
    private final ObjectMapper objectMapper;
    private final JsonNode baseTemplateJsonNode;
    private final List<FieldConfig> autoIncrementFields;
    private final List<FieldConfig> rotationFields;
    private final Class<T> protobufClass;
    
    public ProtobufMessageGenerator(String templateJson,
                                   List<FieldConfig> autoIncrementFields,
                                   List<FieldConfig> rotationFields,
                                   Class<T> protobufClass) {
        super(templateJson, autoIncrementFields, rotationFields);
        this.objectMapper = new ObjectMapper();
        try {
            this.baseTemplateJsonNode = objectMapper.readTree(templateJson);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse template JSON", e);
        }
        this.autoIncrementFields = autoIncrementFields;
        this.rotationFields = rotationFields;
        this.protobufClass = protobufClass;
    }
    
    @Override
    public Object[] generateMessageAndJson(int messageIndex) {
        try {
            ObjectNode objectNode = objectMapper.createObjectNode();
            objectNode.setAll((ObjectNode) baseTemplateJsonNode);
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
            String json = objectMapper.writeValueAsString(objectNode);
            T protobufMessage = buildProtobufFromJson(objectNode);
            return new Object[]{protobufMessage, json};
        } catch (Exception e) {
            logger.error("Error in generateMessageAndJson #{}: {}", messageIndex, e.getMessage(), e);
            throw new RuntimeException("Failed to generate Protobuf message", e);
        }
    }

    
    @SuppressWarnings("unchecked")
    private T buildProtobufFromJson(JsonNode jsonNode) throws Exception {
        Method newBuilderMethod = protobufClass.getMethod("newBuilder");
        Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
        JsonFormat.parser().ignoringUnknownFields().merge(jsonNode.toString(), builder);
        return (T) builder.build();
    }
    
}
