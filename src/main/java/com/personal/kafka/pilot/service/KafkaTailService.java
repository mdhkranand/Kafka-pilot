package com.personal.kafka.pilot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
 */
public class KafkaTailService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTailService.class);
    private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final DateTimeFormatter DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int MAX_MESSAGES = 200;
    private static final long MAX_DURATION_MS = 5 * 60 * 1000L; // 5 minutes
    private static final long POLL_INTERVAL_MS = 5_000L;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(2000);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private volatile ClassLoader customClassLoader;

    public void setCustomClassLoader(ClassLoader classLoader) {
        this.customClassLoader = classLoader;
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
     */
    public void tail(String bootstrapServers, String topic,
                     String keyDeserializer, String valueDeserializer,
                     Integer specificPartition, String searchText,
                     AtomicBoolean stopFlag, TailCallback callback) {

        final String resolvedKeyDeser = resolve(keyDeserializer);
        final String resolvedValDeser = resolve(valueDeserializer);
        final boolean hasFilter = searchText != null && !searchText.trim().isEmpty();
        final String filter = hasFilter ? searchText.trim() : null;

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
                    if (hasFilter && !valueStr.toLowerCase().contains(filter.toLowerCase())) continue;

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
            callback.onStop("error: " + e.getMessage(), 0);
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

    private String formatValue(Object value) {
        if (value == null) return "null";
        return value.toString();
    }

    private String resolve(String input) {
        return (input == null || input.trim().isEmpty()) ? DEFAULT_DESERIALIZER : input.trim();
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
