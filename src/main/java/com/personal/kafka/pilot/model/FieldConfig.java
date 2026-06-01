package com.personal.kafka.pilot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FieldConfig {
    
    private String fieldPath;
    
    private FieldType fieldType;
    
    private long startValue;
    
    private long incrementStep;
    
    private List<Object> rotationValues;
    
    private int currentRotationIndex;
    
    public enum FieldType {
        AUTO_INCREMENT,
        ROTATION,
        RANDOM,
        UUID
    }
    
    public Object peekNextValue(int messageIndex) {
        if (fieldType == null) return null;
        switch (fieldType) {
            case AUTO_INCREMENT:
                return startValue + (long) messageIndex * incrementStep;
            case ROTATION:
                if (rotationValues == null || rotationValues.isEmpty()) return null;
                return rotationValues.get(messageIndex % rotationValues.size());
            case RANDOM:
                if (rotationValues == null || rotationValues.isEmpty()) return null;
                return rotationValues.get(messageIndex % rotationValues.size());
            case UUID:
                return java.util.UUID.nameUUIDFromBytes(
                    (fieldPath + messageIndex).getBytes()).toString();
            default:
                return null;
        }
    }

    public Object getNextValue() {
        if (fieldType == null) {
            return null;
        }
        switch (fieldType) {
            case AUTO_INCREMENT:
                long value = startValue;
                startValue += incrementStep;
                return value;
            case ROTATION:
                if (rotationValues == null || rotationValues.isEmpty()) {
                    return null;
                }
                Object rotValue = rotationValues.get(currentRotationIndex);
                currentRotationIndex = (currentRotationIndex + 1) % rotationValues.size();
                return rotValue;
            case RANDOM:
                if (rotationValues == null || rotationValues.isEmpty()) {
                    return null;
                }
                return rotationValues.get((int) (Math.random() * rotationValues.size()));
            case UUID:
                return UUID.randomUUID().toString();
            default:
                return null;
        }
    }
}
