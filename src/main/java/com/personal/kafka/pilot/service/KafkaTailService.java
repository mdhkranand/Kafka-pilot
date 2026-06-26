package com.personal.kafka.pilot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Service for live-tailing a Kafka topic.
 * Polls forward from the latest offsets at start time, respects text filter and
 * partition scope, stops after maxMessages or maxDurationMs — whichever comes first.
 * Supports FlatBuffer and Protobuf deserialization like KafkaSearchService.
 */
public class KafkaTailService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTailService.class);
    private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final DateTimeFormatter DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int MAX_MESSAGES = 200;
    private static final long MAX_DURATION_MS = 10 * 60 * 1000L; // 10 minutes
    private static final long POLL_INTERVAL_MS = 5_000L;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(2000);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final java.util.Set<String> FLATBUF_EXCLUDED_METHODS = new java.util.HashSet<>(java.util.Arrays.asList(
            "getClass", "hashCode", "toString", "wait", "notify", "notifyAll",
            "keysVector", "valuesVector"
    ));

    private volatile ClassLoader customClassLoader;
    private volatile String activeFlatbufClassName;
    private volatile boolean decodeAsFlatbuf;

    public void setCustomClassLoader(ClassLoader classLoader) {
        this.customClassLoader = classLoader;
    }

    public void setActiveFlatbufClassName(String className) {
        this.activeFlatbufClassName = (className != null && !className.trim().isEmpty()) ? className.trim() : null;
    }

    public void setDecodeAsFlatbuf(boolean decode) {
        this.decodeAsFlatbuf = decode;
    }

    /**
     * Callback interface — called after each poll with the batch of new formatted messages
     * (newest-first within the batch) and the running total count.
     */
    public interface TailCallback {
        void onBatch(List<String> messages, int totalSoFar);
        void onStop(String reason, int total);
    }

    /**
     * Starts the tail loop on the calling thread (run inside a dedicated Thread from the controller).
     *
     * @param bootstrapServers  Kafka brokers
     * @param topic             Topic to tail
     * @param keyDeserializer   Key deserializer class (null = StringDeserializer)
     * @param valueDeserializer Value deserializer class (null = StringDeserializer)
     * @param specificPartition null = all partitions
     * @param searchText        null or empty = no filter
     * @param stopFlag          Set to true externally to stop early
     * @param callback          UI callback — always invoked on the service thread; caller must Platform.runLater
     * @param uiLogger          Optional UI console logger
     */
    public void tail(String bootstrapServers, String topic,
                     String keyDeserializer, String valueDeserializer,
                     Integer specificPartition, String searchText,
                     AtomicBoolean stopFlag, TailCallback callback,
                     java.util.function.Consumer<String> uiLogger) {

        final String resolvedKeyDeser = resolveDeserializer(keyDeserializer);
        final String resolvedValDeser = resolveValueDeserializer(valueDeserializer);
        final boolean hasFilter = searchText != null && !searchText.trim().isEmpty();
        final String filter = hasFilter ? searchText.trim() : null;

        logUi(uiLogger, "[Tail] Starting tail for topic: " + topic + 
              (specificPartition != null ? " partition=" + specificPartition : " all partitions") +
              (hasFilter ? " filter=\"" + filter + "\"" : ""));

        Properties props = buildProps(bootstrapServers, resolvedKeyDeser, resolvedValDeser, topic);
        if (customClassLoader != null) {
            Thread.currentThread().setContextClassLoader(customClassLoader);
        }

        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {

            // --- Discover partitions ---
            List<TopicPartition> partitions = new ArrayList<>();
            if (specificPartition != null) {
                partitions.add(new TopicPartition(topic, specificPartition));
            } else {
                for (PartitionInfo pi : consumer.partitionsFor(topic)) {
                    partitions.add(new TopicPartition(topic, pi.partition()));
                }
            }
            logUi(uiLogger, "[Tail] Monitoring " + partitions.size() + " partition(s)");
            consumer.assign(partitions);

            // --- Seek to current end (tail only new messages) ---
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            for (TopicPartition tp : partitions) {
                long end = endOffsets.getOrDefault(tp, 0L);
                consumer.seek(tp, end);
            }

            // --- Track last seen offsets per partition for resume after each poll ---
            Map<TopicPartition, Long> nextOffsets = new HashMap<>(endOffsets);

            int totalCollected = 0;
            long startTime = System.currentTimeMillis();

            while (!stopFlag.get()) {
                // Check limits
                if (totalCollected >= MAX_MESSAGES) {
                    callback.onStop("message limit reached (" + MAX_MESSAGES + ")", totalCollected);
                    return;
                }
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= MAX_DURATION_MS) {
                    callback.onStop("time limit reached (5 min)", totalCollected);
                    return;
                }

                // Re-seek to next expected offsets (safety — avoids duplicate delivery on re-assign)
                for (TopicPartition tp : partitions) {
                    consumer.seek(tp, nextOffsets.getOrDefault(tp, endOffsets.getOrDefault(tp, 0L)));
                }

                ConsumerRecords<String, Object> records = consumer.poll(POLL_TIMEOUT);

                // Collect matching records from this poll, newest first within the batch
                List<RecordEntry> batch = new ArrayList<>();
                for (ConsumerRecord<String, Object> rec : records) {
                    if (stopFlag.get()) break;
                    // Update next offset
                    nextOffsets.merge(new TopicPartition(rec.topic(), rec.partition()),
                            rec.offset() + 1, Math::max);

                    String valueStr = formatValue(rec.value());
                    if (hasFilter && !matchesFilter(valueStr, filter)) continue;

                    batch.add(new RecordEntry(rec, valueStr));
                }

                if (!batch.isEmpty()) {
                    // Sort newest first
                    batch.sort((a, b) -> Long.compare(b.timestamp, a.timestamp));

                    int remaining = MAX_MESSAGES - totalCollected;
                    if (batch.size() > remaining) batch = batch.subList(0, remaining);

                    List<String> formatted = new ArrayList<>(batch.size());
                    for (RecordEntry e : batch) {
                        formatted.add(formatRecord(e));
                    }
                    totalCollected += formatted.size();
                    callback.onBatch(formatted, totalCollected);
                }

                if (stopFlag.get()) break;

                // Sleep between polls (interruptible)
                long sleepEnd = System.currentTimeMillis() + POLL_INTERVAL_MS;
                while (System.currentTimeMillis() < sleepEnd && !stopFlag.get()) {
                    try { Thread.sleep(200); } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            callback.onStop(stopFlag.get() ? "stopped by user" : "completed", totalCollected);

        } catch (Exception e) {
            logger.error("Tail error for topic {}: {}", topic, e.getMessage(), e);
            logUi(uiLogger, "[Tail] ERROR: " + e.getMessage());
            callback.onStop("error: " + e.getMessage(), 0);
        }
    }

    /**
     * Helper method to log to UI console if callback is provided
     */
    private void logUi(java.util.function.Consumer<String> uiLogger, String message) {
        if (uiLogger != null) {
            uiLogger.accept(message);
        }
    }

    // --- Helpers ---

    private static class RecordEntry {
        final ConsumerRecord<String, Object> rec;
        final String valueStr;
        final long timestamp;

        RecordEntry(ConsumerRecord<String, Object> rec, String valueStr) {
            this.rec = rec;
            this.valueStr = valueStr;
            this.timestamp = rec.timestamp();
        }
    }

    private String formatRecord(RecordEntry e) {
        ConsumerRecord<String, Object> rec = e.rec;

        // Build JSON object for the result
        Map<String, Object> resultMap = new LinkedHashMap<>();
        String ts = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(rec.timestamp()), ZoneId.systemDefault()).format(DISPLAY_FORMAT);
        resultMap.put("timestamp", ts);
        resultMap.put("timestampEpochMs", rec.timestamp());
        resultMap.put("timestampType", rec.timestampType() != null ? rec.timestampType().name() : "UNKNOWN");
        resultMap.put("partition", rec.partition());
        resultMap.put("offset", rec.offset());
        resultMap.put("key", rec.key());
        resultMap.put("headers", parseHeadersToMap(rec.headers()));
        // Parse value as JSON object if possible for proper formatting
        resultMap.put("value", parseValueToJson(e.valueStr));

        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(resultMap);
        } catch (Exception ex) {
            return "{\"error\": \"Failed to format as JSON\"}";
        }
    }

    /**
     * Parses a string value to JSON object/array if valid JSON, otherwise returns as-is.
     */
    private Object parseValueToJson(String value) {
        if (value == null || value.trim().isEmpty()) {
            return value;
        }
        try {
            String trimmed = value.trim();
            if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
                return objectMapper.readValue(value, Object.class);
            }
        } catch (Exception e) {
            // Not valid JSON, return as string
        }
        return value;
    }

    private Map<String, String> parseHeadersToMap(org.apache.kafka.common.header.Headers headers) {
        Map<String, String> map = new LinkedHashMap<>();
        if (headers != null) {
            for (org.apache.kafka.common.header.Header h : headers) {
                map.put(h.key(), h.value() != null ? new String(h.value()) : null);
            }
        }
        return map.isEmpty() ? null : map;
    }

    private String formatValue(Object val) {
        if (val instanceof Message) {
            try {
                return JsonFormat.printer().preservingProtoFieldNames().print((Message) val);
            } catch (Exception e) {
                return "[ProtobufMessage: " + val.getClass().getSimpleName() + " - " + e.getClass().getSimpleName() + "]";
            }
        }
        if (val instanceof byte[]) {
            if (decodeAsFlatbuf) return formatFlatBufferBytes((byte[]) val);
            byte[] b = (byte[]) val;
            StringBuilder hex = new StringBuilder("[bytes (").append(b.length).append(")] ");
            int preview = Math.min(b.length, 64);
            for (int i = 0; i < preview; i++) { hex.append(String.format("%02x", b[i])); if (i < preview - 1) hex.append(' '); }
            if (b.length > preview) hex.append(" ...");
            return hex.toString();
        }
        if (val instanceof String) {
            String str = (String) val;
            try {
                Object json = objectMapper.readValue(str, Object.class);
                return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
            } catch (Exception e) {
                return str;
            }
        }
        return val == null ? "null" : val.toString();
    }

    private String resolveDeserializer(String input) {
        return (input == null || input.trim().isEmpty()) ? DEFAULT_DESERIALIZER : input.trim();
    }

    private String resolveValueDeserializer(String input) {
        if (decodeAsFlatbuf) return "org.apache.kafka.common.serialization.ByteArrayDeserializer";
        return resolveDeserializer(input);
    }

    // --- FlatBuffer Decoding Methods (copied from KafkaSearchService) ---

    private String formatFlatBufferBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "(empty)";

        // Try stripping a possible length-prefix (4-byte int header used by some Kafka serializers)
        if (bytes.length > 4) {
            ByteBuffer probe = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int possibleLen = probe.getInt(0);
            if (possibleLen > 0 && possibleLen == bytes.length - 4) {
                byte[] stripped = Arrays.copyOfRange(bytes, 4, bytes.length);
                String decoded = tryDecodeFlatBuffer(stripped, customClassLoader);
                if (decoded != null) return decoded;
            }
        }

        String decoded = tryDecodeFlatBuffer(bytes, customClassLoader);
        if (decoded != null) return decoded;

        // Not a decodable FlatBuffer — the bytes may actually be plain UTF-8 JSON/text
        String asText = tryDecodeUtf8Json(bytes);
        if (asText != null) return asText;

        StringBuilder hex = new StringBuilder("[FlatBuffer bytes (").append(bytes.length).append(")] ");
        int preview = Math.min(bytes.length, 64);
        for (int i = 0; i < preview; i++) {
            hex.append(String.format("%02x", bytes[i]));
            if (i < preview - 1) hex.append(' ');
        }
        if (bytes.length > preview) hex.append(" ...");
        return hex.toString();
    }

    private String tryDecodeUtf8Json(byte[] bytes) {
        try {
            String s = new String(bytes, java.nio.charset.StandardCharsets.UTF_8).trim();
            if (s.isEmpty()) return null;
            char c = s.charAt(0);
            if (c != '{' && c != '[') return null;
            Object json = objectMapper.readValue(s, Object.class);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
        } catch (Exception e) {
            return null;
        }
    }

    private String tryDecodeFlatBuffer(byte[] bytes, ClassLoader cl) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

            if (activeFlatbufClassName != null) {
                try {
                    ClassLoader loader = cl != null ? cl : Thread.currentThread().getContextClassLoader();
                    Class<?> clazz = Class.forName(activeFlatbufClassName, true, loader);
                    Object table = tryGetRootObject(clazz, buf.duplicate());
                    if (table != null) {
                        String json = flatTableToJson(table, clazz);
                        if (!isEmptyResult(json)) return json;
                    }
                    String json = parseFlatBufferDirect(bytes, clazz);
                    if (json != null) return json;
                } catch (Exception e) {
                    logger.debug("Could not decode FlatBuffer with class '{}': {}", activeFlatbufClassName, e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.debug("FlatBuffer decode attempt failed: {}", e.getMessage());
        }
        return null;
    }

    private boolean isEmptyResult(String json) {
        if (json == null) return true;
        String stripped = json.replaceAll("\"[^\"]+\"\\s*:\\s*", "")
                              .replaceAll("[{}\\[\\],\\s]", "");
        return stripped.replaceAll("null|false|true|0", "").isEmpty();
    }

    private Object tryGetRootObject(Class<?> clazz, ByteBuffer buf) {
        try {
            Method m = clazz.getMethod("getRootAs" + clazz.getSimpleName(), ByteBuffer.class);
            return m.invoke(null, buf);
        } catch (Exception e) {
            return null;
        }
    }

    private String flatTableToJson(Object table, Class<?> clazz) {
        try {
            Class<?> actualClass = table.getClass();
            String topLevelClassName = actualClass.getName();
            Map<String, Object> map = new LinkedHashMap<>();

            // Build maps for vector handling: fieldName -> Length method, fieldName -> indexed accessor
            Map<String, Method> lengthMethods = new HashMap<>();
            Map<String, Method> vectorAccessors = new HashMap<>();
            for (Method m : actualClass.getMethods()) {
                if (!m.getDeclaringClass().getName().equals(topLevelClassName)) continue;
                if (m.getName().startsWith("__") || m.getName().startsWith("mutate")) continue;
                if (m.getName().endsWith("Length") && m.getParameterCount() == 0 && m.getReturnType() == int.class) {
                    String baseName = m.getName().substring(0, m.getName().length() - 6); // remove "Length"
                    lengthMethods.put(baseName, m);
                }
                if (m.getParameterCount() == 1 && m.getParameterTypes()[0] == int.class) {
                    vectorAccessors.put(m.getName(), m);
                }
            }

            for (Method m : actualClass.getMethods()) {
                if (m.getParameterCount() != 0) continue;
                if (FLATBUF_EXCLUDED_METHODS.contains(m.getName())) continue;
                if (m.getName().startsWith("__")) continue;
                if (m.getName().startsWith("mutate")) continue;
                if (!m.getDeclaringClass().getName().equals(topLevelClassName)) continue;
                Class<?> ret = m.getReturnType();
                if (ret == void.class) continue;
                if (ByteBuffer.class.isAssignableFrom(ret)) continue;
                String fieldName = m.getName();
                try {
                    Object v = m.invoke(table);

                    // Check if this is a vector length field with a corresponding accessor
                    String vectorBaseName = null;
                    if (fieldName.endsWith("Length") && fieldName.length() > 6) {
                        vectorBaseName = fieldName.substring(0, fieldName.length() - 6); // remove "Length"
                    }
                    if (vectorBaseName != null && vectorAccessors.containsKey(vectorBaseName) && v instanceof Integer) {
                        int vecLen = (Integer) v;
                        Method accessor = vectorAccessors.get(vectorBaseName);
                        Class<?> elementType = accessor.getReturnType();
                        java.util.List<Object> elements = new java.util.ArrayList<>();
                        for (int i = 0; i < vecLen && i < 1000; i++) { // cap at 1000 elements
                            try {
                                Object elem = accessor.invoke(table, i);
                                if (elem != null) {
                                    // Check if element is a nested FlatBuffer Table
                                    boolean isNestedTable = false;
                                    for (Class<?> iface : elem.getClass().getSuperclass() != null
                                            ? new Class[]{elem.getClass(), elem.getClass().getSuperclass()} : new Class[]{elem.getClass()}) {
                                        if (iface.getName().equals("com.google.flatbuffers.Table")) { isNestedTable = true; break; }
                                    }
                                    if (!isNestedTable) {
                                        Class<?> sc = elem.getClass().getSuperclass();
                                        while (sc != null && !isNestedTable) {
                                            if (sc.getName().equals("com.google.flatbuffers.Table")) isNestedTable = true;
                                            sc = sc.getSuperclass();
                                        }
                                    }
                                    if (isNestedTable) {
                                        String nested = flatTableToJson(elem, elem.getClass());
                                        try { elem = objectMapper.readValue(nested, Object.class); } catch (Exception e) { elem = nested; }
                                    }
                                }
                                elements.add(elem);
                            } catch (Exception e) {
                                elements.add(null);
                            }
                        }
                        map.put(fieldName, vecLen);  // e.g., "resourceTagKeysLength": 3
                        map.put(vectorBaseName, elements);  // e.g., "resourceTagKeys": [...]
                    } else {
                        // Normal field handling - recurse into nested objects that look like FlatBuffer Tables
                        if (v != null) {
                            boolean isNestedTable = false;
                            for (Class<?> iface : v.getClass().getSuperclass() != null
                                    ? new Class[]{v.getClass(), v.getClass().getSuperclass()} : new Class[]{v.getClass()}) {
                                if (iface.getName().equals("com.google.flatbuffers.Table")) { isNestedTable = true; break; }
                            }
                            if (!isNestedTable) {
                                Class<?> sc = v.getClass().getSuperclass();
                                while (sc != null && !isNestedTable) {
                                    if (sc.getName().equals("com.google.flatbuffers.Table")) isNestedTable = true;
                                    sc = sc.getSuperclass();
                                }
                            }
                            if (isNestedTable) {
                                String nested = flatTableToJson(v, v.getClass());
                                try { v = objectMapper.readValue(nested, Object.class); } catch (Exception e) { v = nested; }
                            }
                        }
                        map.put(fieldName, v);
                    }
                } catch (Exception ignored) {}
            }
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
        } catch (Exception e) {
            return null;
        }
    }

    private String parseFlatBufferDirect(byte[] bytes, Class<?> clazz) {
        try {
            ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int rootOffset = bb.getInt(0);
            int vtableOffset = rootOffset - bb.getInt(rootOffset);
            int vtableSize = bb.getShort(vtableOffset) & 0xFFFF;

            Map<Integer, Method> slotToMethod = buildSlotMap(clazz);
            if (slotToMethod.isEmpty()) return null;

            // Build maps for vector handling: fieldName -> Length method, fieldName -> indexed accessor
            Map<String, Method> lengthMethods = new HashMap<>();
            Map<String, Method> vectorAccessors = new HashMap<>();
            for (Method m : clazz.getMethods()) {
                if (!m.getDeclaringClass().getName().equals(clazz.getName())) continue;
                if (m.getName().startsWith("__") || m.getName().startsWith("mutate")) continue;
                if (m.getName().endsWith("Length") && m.getParameterCount() == 0 && m.getReturnType() == int.class) {
                    String baseName = m.getName().substring(0, m.getName().length() - 6); // remove "Length"
                    lengthMethods.put(baseName, m);
                }
                if (m.getParameterCount() == 1 && m.getParameterTypes()[0] == int.class) {
                    vectorAccessors.put(m.getName(), m);
                }
            }

            Map<String, Object> map = new LinkedHashMap<>();
            for (Map.Entry<Integer, Method> entry : slotToMethod.entrySet()) {
                int slot = entry.getKey();
                Method m = entry.getValue();
                String fieldName = m.getName();
                if (slot >= vtableSize) {
                    map.put(fieldName, null);
                    continue;
                }
                int fieldOffset = bb.getShort(vtableOffset + slot) & 0xFFFF;
                Class<?> ret = m.getReturnType();
                if (fieldOffset == 0) {
                    if (ret == boolean.class) map.put(fieldName, false);
                    else if (ret == long.class || ret == int.class || ret == short.class || ret == byte.class) map.put(fieldName, 0);
                    else map.put(fieldName, null);
                    continue;
                }
                int abs = rootOffset + fieldOffset;
                try {
                    // Check if this field is a vector length field (ends with "Length")
                    // and there's a corresponding indexed accessor for the base name
                    String vectorBaseName = null;
                    if (fieldName.endsWith("Length") && fieldName.length() > 6) {
                        vectorBaseName = fieldName.substring(0, fieldName.length() - 6); // remove "Length"
                    }
                    if (vectorBaseName != null && vectorAccessors.containsKey(vectorBaseName)) {
                        // Read vector: [length: int] [element offsets or values...]
                        int vecLen = bb.getInt(abs);
                        Method accessor = vectorAccessors.get(vectorBaseName);
                        Class<?> elementType = accessor.getReturnType();
                        java.util.List<Object> elements = new java.util.ArrayList<>();
                        int elemOffset = abs + 4; // skip length field
                        for (int i = 0; i < vecLen && i < 1000; i++) { // cap at 1000 elements
                            if (elementType == String.class) {
                                int elemAbs = elemOffset + bb.getInt(elemOffset);
                                int strLen = bb.getInt(elemAbs);
                                byte[] strBytes = new byte[strLen];
                                bb.position(elemAbs + 4);
                                bb.get(strBytes);
                                elements.add(new String(strBytes, java.nio.charset.StandardCharsets.UTF_8));
                            } else if (elementType == long.class) {
                                elements.add(bb.getLong(elemOffset));
                            } else if (elementType == int.class) {
                                elements.add(bb.getInt(elemOffset));
                            } else if (elementType == short.class) {
                                elements.add((int) bb.getShort(elemOffset));
                            } else if (elementType == byte.class) {
                                elements.add((int) bb.get(elemOffset));
                            } else if (elementType == boolean.class) {
                                elements.add(bb.get(elemOffset) != 0);
                            } else if (elementType == float.class) {
                                elements.add(bb.getFloat(elemOffset));
                            } else if (elementType == double.class) {
                                elements.add(bb.getDouble(elemOffset));
                            } else {
                                // Nested table/object - read as nested structure
                                int nestedOffset = bb.getInt(elemOffset);
                                if (nestedOffset != 0) {
                                    int nestedAbs = elemOffset + nestedOffset;
                                    byte[] nestedBytes = extractObjectBytes(bb, nestedAbs);
                                    String nestedJson = parseFlatBufferDirect(nestedBytes, elementType);
                                    if (nestedJson != null) {
                                        elements.add(objectMapper.readValue(nestedJson, Object.class));
                                    } else {
                                        elements.add(null);
                                    }
                                } else {
                                    elements.add(null);
                                }
                            }
                            elemOffset += 4; // each offset/element is 4 bytes
                        }
                        map.put(fieldName, vecLen);  // e.g., "resourceTagKeysLength": 3
                        map.put(vectorBaseName, elements);  // e.g., "resourceTagKeys": [...]
                    } else if (ret == String.class) {
                        int strAbs = abs + bb.getInt(abs);
                        int strLen = bb.getInt(strAbs);
                        byte[] strBytes = new byte[strLen];
                        bb.position(strAbs + 4);
                        bb.get(strBytes);
                        map.put(fieldName, new String(strBytes, java.nio.charset.StandardCharsets.UTF_8));
                    } else if (ret == long.class) { map.put(fieldName, bb.getLong(abs)); }
                    else if (ret == int.class) { map.put(fieldName, bb.getInt(abs)); }
                    else if (ret == short.class) { map.put(fieldName, (int) bb.getShort(abs)); }
                    else if (ret == byte.class) { map.put(fieldName, (int) bb.get(abs)); }
                    else if (ret == boolean.class) { map.put(fieldName, bb.get(abs) != 0); }
                    else if (ret == float.class) { map.put(fieldName, bb.getFloat(abs)); }
                    else if (ret == double.class) { map.put(fieldName, bb.getDouble(abs)); }
                } catch (Exception e) {
                    map.put(fieldName, null);
                }
            }
            if (map.isEmpty()) return null;
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
        } catch (Exception e) {
            return null;
        }
    }

    /** Extracts a nested object's bytes from the buffer for recursive parsing */
    private byte[] extractObjectBytes(ByteBuffer bb, int offset) {
        int vtableRelOffset = bb.getInt(offset);
        int vtableOffset = offset - vtableRelOffset;
        int vtableSize = bb.getShort(vtableOffset) & 0xFFFF;
        int estimatedSize = Math.min(vtableSize * 4 + 8, bb.capacity() - offset);
        byte[] result = new byte[estimatedSize];
        bb.position(offset);
        bb.get(result);
        return result;
    }

    private Map<Integer, Method> buildSlotMap(Class<?> clazz) {
        Map<Integer, Method> slotMap = new java.util.TreeMap<>();
        Map<String, Method> methodIndex = new HashMap<>();
        for (Method m : clazz.getMethods()) {
            if (m.getParameterCount() != 0) continue;
            if (!m.getDeclaringClass().getName().equals(clazz.getName())) continue;
            if (m.getName().startsWith("__") || m.getName().startsWith("mutate")) continue;
            if (FLATBUF_EXCLUDED_METHODS.contains(m.getName())) continue;
            Class<?> ret = m.getReturnType();
            if (ret == void.class || ByteBuffer.class.isAssignableFrom(ret)) continue;
            if (m.getName().endsWith("Length")) continue;
            methodIndex.put(m.getName(), m);
        }

        String resourceName = clazz.getName().replace('.', '/') + ".class";
        byte[] classBytes = null;
        try (java.io.InputStream is = clazz.getClassLoader().getResourceAsStream(resourceName)) {
            if (is != null) classBytes = is.readAllBytes();
        } catch (Exception e) {
            return slotMap;
        }
        if (classBytes == null) return slotMap;

        try {
            java.nio.ByteBuffer cb = java.nio.ByteBuffer.wrap(classBytes);
            cb.order(java.nio.ByteOrder.BIG_ENDIAN);
            cb.getInt();
            cb.getShort(); cb.getShort();
            int cpCount = cb.getShort() & 0xFFFF;
            Object[] cp = new Object[cpCount];
            for (int i = 1; i < cpCount; i++) {
                int tag = cb.get() & 0xFF;
                switch (tag) {
                    case 1: { int len = cb.getShort() & 0xFFFF; byte[] s = new byte[len]; cb.get(s); cp[i] = new String(s, java.nio.charset.StandardCharsets.UTF_8); break; }
                    case 3: case 4: cb.getInt(); break;
                    case 5: case 6: cb.getLong(); i++; break;
                    case 7: case 8: case 16: case 19: case 20: cb.getShort(); break;
                    case 9: case 10: case 11: case 12: case 17: case 18: cb.getInt(); break;
                    case 15: cb.get(); cb.getShort(); break;
                    default: break;
                }
            }
            cb.getShort();
            cb.getShort();
            cb.getShort();
            int ifCount = cb.getShort() & 0xFFFF;
            for (int i = 0; i < ifCount; i++) cb.getShort();
            int fieldCount = cb.getShort() & 0xFFFF;
            for (int i = 0; i < fieldCount; i++) {
                cb.getShort(); cb.getShort(); cb.getShort();
                int attrCount = cb.getShort() & 0xFFFF;
                for (int a = 0; a < attrCount; a++) { cb.getShort(); int len = cb.getInt(); cb.position(cb.position() + len); }
            }
            int methodCount = cb.getShort() & 0xFFFF;
            for (int mi = 0; mi < methodCount; mi++) {
                cb.getShort();
                int nameIdx = cb.getShort() & 0xFFFF;
                cb.getShort();
                String mName = (cp[nameIdx] instanceof String) ? (String) cp[nameIdx] : null;
                int attrCount = cb.getShort() & 0xFFFF;
                byte[] codeBytes = null;
                for (int a = 0; a < attrCount; a++) {
                    int attrNameIdx = cb.getShort() & 0xFFFF;
                    int attrLen = cb.getInt();
                    String attrName = (cp[attrNameIdx] instanceof String) ? (String) cp[attrNameIdx] : "";
                    if ("Code".equals(attrName)) {
                        codeBytes = new byte[attrLen];
                        cb.get(codeBytes);
                    } else {
                        cb.position(cb.position() + attrLen);
                    }
                }
                if (mName == null || !methodIndex.containsKey(mName) || codeBytes == null) continue;
                int slot = scanForOffsetSlot(codeBytes);
                if (slot > 0) slotMap.put(slot, methodIndex.get(mName));
            }
        } catch (Exception e) {
            logger.debug("Bytecode slot scan failed for {}: {}", clazz.getSimpleName(), e.getMessage());
        }
        return slotMap;
    }

    private int scanForOffsetSlot(byte[] code) {
        if (code.length < 8) return -1;
        java.nio.ByteBuffer cb = java.nio.ByteBuffer.wrap(code);
        cb.getShort(); cb.getShort();
        int codeLen = cb.getInt();
        int lastPushedInt = -1;
        int end = cb.position() + codeLen;
        while (cb.position() < end) {
            int op = cb.get() & 0xFF;
            if (op == 0x10) { lastPushedInt = cb.get(); }
            else if (op == 0x11) { lastPushedInt = cb.getShort(); }
            else if (op >= 0x02 && op <= 0x08) { lastPushedInt = op - 3; }
            else if (op == 0xB6 || op == 0xB7 || op == 0xB8 || op == 0xB9) {
                int cpRef = cb.getShort() & 0xFFFF;
                if (lastPushedInt > 0 && lastPushedInt % 2 == 0 && lastPushedInt >= 4) return lastPushedInt;
                lastPushedInt = -1;
            }
        }
        return -1;
    }

    private boolean matchesFilter(String value, String filter) {
        if (filter == null || filter.trim().isEmpty()) return true;
        String trimmed = filter.trim();
        String valueLower = value.toLowerCase();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            String inner = trimmed.substring(1, trimmed.length() - 1);
            for (String term : inner.split(",", -1)) {
                if (!valueLower.contains(term.trim().toLowerCase())) return false;
            }
            return true;
        }
        if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
            String inner = trimmed.substring(1, trimmed.length() - 1);
            for (String term : inner.split(",", -1)) {
                if (valueLower.contains(term.trim().toLowerCase())) return true;
            }
            return false;
        }
        return valueLower.contains(trimmed.toLowerCase());
    }

    private Properties buildProps(String bootstrapServers, String keyDeser, String valDeser, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "tail-" + topic + "-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeser);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeser);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return props;
    }
}
