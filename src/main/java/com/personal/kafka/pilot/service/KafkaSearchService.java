package com.personal.kafka.pilot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Service responsible for Kafka consumer operations (search and peek).
 * Follows Single Responsibility Principle — no UI concerns.
 */
public class KafkaSearchService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSearchService.class);
    private static final String DEFAULT_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final DateTimeFormatter DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private volatile ClassLoader customClassLoader;
    private volatile String activeFlatbufClassName;
    private volatile boolean decodeAsFlatbuf;

    public void setActiveFlatbufClassName(String className) {
        this.activeFlatbufClassName = (className != null && !className.trim().isEmpty()) ? className.trim() : null;
    }

    public void setDecodeAsFlatbuf(boolean decode) {
        this.decodeAsFlatbuf = decode;
    }

    public void setCustomClassLoader(ClassLoader classLoader) {
        this.customClassLoader = classLoader;
    }

    public ClassLoader getCustomClassLoader() {
        return customClassLoader;
    }

    /**
     * Peeks one message from a topic (latest or at specific offset).
     * If no partition specified, tries partitions sequentially (0, 1, 2...) until a message is found.
     */
    public PeekResult peekOne(String bootstrapServers, String topic, String keyDeserializer,
                              String valueDeserializer, Integer partition, Long offset,
                              Long fromMs, Long toMs, String searchText) {
        keyDeserializer = resolveDeserializer(keyDeserializer);
        valueDeserializer = resolveValueDeserializer(valueDeserializer);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "peek-" + topic + "-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        if (customClassLoader != null) {
            Thread.currentThread().setContextClassLoader(customClassLoader);
        }

        logger.info("Peek: topic={}, partition={}, offset={}, fromMs={}, toMs={}, filter={}",
                topic, partition, offset, fromMs, toMs, searchText);
        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> allPartitionInfos = consumer.partitionsFor(topic);
            if (allPartitionInfos == null || allPartitionInfos.isEmpty()) {
                return PeekResult.error("Topic not found");
            }

            // Case 1: specific partition + offset — return exactly that one message
            if (partition != null && offset != null) {
                TopicPartition tp = new TopicPartition(topic, partition);
                consumer.assign(Collections.singletonList(tp));
                consumer.seek(tp, offset);
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(10));
                for (ConsumerRecord<String, Object> rec : records) {
                    if (rec.offset() == offset) return formatPeekResult(rec, partition, offset);
                }
                return PeekResult.error("No message at offset " + offset + " in partition " + partition);
            }

            // Case 2+3: no specific offset — assign all (or specific) partitions and scan
            List<TopicPartition> tps = new ArrayList<>();
            if (partition != null) {
                tps.add(new TopicPartition(topic, partition));
            } else {
                for (PartitionInfo pi : allPartitionInfos) {
                    tps.add(new TopicPartition(topic, pi.partition()));
                }
            }
            consumer.assign(tps);

            if (fromMs != null) {
                Map<TopicPartition, Long> tsMap = new HashMap<>();
                for (TopicPartition tp : tps) tsMap.put(tp, fromMs);
                Map<TopicPartition, OffsetAndTimestamp> tsOffsets = consumer.offsetsForTimes(tsMap);
                for (TopicPartition tp : tps) {
                    OffsetAndTimestamp ots = tsOffsets.get(tp);
                    if (ots != null) consumer.seek(tp, ots.offset());
                    else consumer.seekToEnd(Collections.singletonList(tp));
                }
            } else {
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tps);
                Map<TopicPartition, Long> beginOffsets = consumer.beginningOffsets(tps);
                for (TopicPartition tp : tps) {
                    long end = endOffsets.getOrDefault(tp, 0L);
                    long begin = beginOffsets.getOrDefault(tp, 0L);
                    // Use larger window (1000) if text filter provided, otherwise 100
                    long scanWindow = (searchText != null && !searchText.trim().isEmpty()) ? TAIL_SCAN_WINDOW : 100L;
                    consumer.seek(tp, Math.max(begin, end - scanWindow));
                }
            }

            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(10));
                if (records.isEmpty()) { emptyPolls++; continue; }
                for (ConsumerRecord<String, Object> rec : records) {
                    if (toMs != null && rec.timestamp() > toMs) continue;
                    String valueStr = formatValue(rec.value());
                    if (!matchesFilter(valueStr, searchText)) continue;
                    return formatPeekResult(rec, rec.partition(), null);
                }
                emptyPolls = 0;
            }

            return PeekResult.error("No messages found");
        } catch (Exception e) {
            logger.error("Peek error", e);
            return PeekResult.error(e.getMessage());
        }
    }

    private PeekResult formatPeekResult(ConsumerRecord<String, Object> rec, int partition, Long offset) {
        String messageValue = null;
        if (!decodeAsFlatbuf) {
            messageValue = (rec.value() instanceof String) ? (String) rec.value() : convertProtobufToJson(rec.value());
        }
        if (messageValue == null) messageValue = formatValue(rec.value());

        String ts = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(rec.timestamp()), ZoneId.systemDefault()).format(DISPLAY_FORMAT);

        String headers = formatHeaders(rec.headers());

        // Build JSON object for the result
        Map<String, Object> resultMap = new LinkedHashMap<>();
        resultMap.put("timestamp", ts);
        resultMap.put("timestampEpochMs", rec.timestamp());
        resultMap.put("timestampType", rec.timestampType() != null ? rec.timestampType().name() : "UNKNOWN");
        resultMap.put("partition", rec.partition());
        resultMap.put("offset", rec.offset());
        resultMap.put("key", rec.key());
        resultMap.put("headers", headers.isEmpty() ? null : parseHeadersToMap(rec.headers()));
        // Parse value as JSON object if possible, otherwise store as string
        resultMap.put("value", parseValueToJson(messageValue));

        String jsonOutput;
        try {
            jsonOutput = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(resultMap);
        } catch (Exception e) {
            jsonOutput = "{\"error\": \"Failed to format as JSON\"}";
        }

        String status = "1 message from partition " + partition
                + (offset != null ? " @ offset " + offset : " (latest)");
        return PeekResult.success(jsonOutput, status, messageValue);
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
        return map;
    }

    private static final int BATCH_SIZE = 30;        // partitions per batch — controls peak memory
    private static final long TAIL_SCAN_WINDOW = 1000L; // messages per partition for text-only scan

    /**
     * Searches messages across partitions using a single reusable thread pool.
     * Uses a fixed-size thread pool (BATCH_SIZE = 30) with a Semaphore to limit
     * concurrent KafkaConsumer instances to 30 at any time, keeping peak memory bounded.
     * Threads are reused across all partitions instead of creating/shutting down per batch.
     *
     * Seek strategy:
     *   specificPartition + specificOffset → seek to exact offset (1 thread)
     *   fromMs set                         → offsetsForTimes per partition
     *   text filter only                   → tail scan (end - 1000) per partition
     *   no filter, no time                 → tail scan (end - maxResults) per partition
     */
    public SearchResult searchMessages(String bootstrapServers, String topic, String keyDeserializer,
                                       String valueDeserializer, Integer specificPartition, Long specificOffset,
                                       Long fromMs, Long toMs,
                                       String searchText, int maxResults, boolean fromBeginning,
                                       Consumer<String> logConsumer, AtomicBoolean stopFlag) {
        final String resolvedKeyDeser = resolveDeserializer(keyDeserializer);
        final String resolvedValDeser = resolveValueDeserializer(valueDeserializer);
        final boolean hasTextFilter = searchText != null && !searchText.trim().isEmpty();
        Consumer<String> safeLog = msg -> { if (logConsumer != null) logConsumer.accept(msg); };

        // --- Step 1: discover partitions + snapshot begin/end offsets (single setup consumer) ---
        List<TopicPartition> allPartitions = new ArrayList<>();
        Map<TopicPartition, Long> endOffsetSnapshot = new HashMap<>();
        Map<TopicPartition, Long> beginOffsetSnapshot = new HashMap<>();
        try {
            Properties adminProps = buildConsumerProps(bootstrapServers, resolvedKeyDeser, resolvedValDeser, topic);
            try (KafkaConsumer<String, Object> setupConsumer = new KafkaConsumer<>(adminProps)) {
                if (specificPartition != null) {
                    allPartitions.add(new TopicPartition(topic, specificPartition));
                } else {
                    for (PartitionInfo pi : setupConsumer.partitionsFor(topic)) {
                        allPartitions.add(new TopicPartition(topic, pi.partition()));
                    }
                }
                setupConsumer.assign(allPartitions);
                endOffsetSnapshot.putAll(setupConsumer.endOffsets(allPartitions));
                beginOffsetSnapshot.putAll(setupConsumer.beginningOffsets(allPartitions));
            }
        } catch (Exception e) {
            logger.error("Failed to setup consumer for topic: {}", topic, e);
            safeLog.accept("[Search] ERROR setting up consumer: " + e.getMessage());
            return SearchResult.error(e.getMessage());
        }
        final Map<TopicPartition, Long> endOffsets = Collections.unmodifiableMap(endOffsetSnapshot);
        final Map<TopicPartition, Long> beginOffsets = Collections.unmodifiableMap(beginOffsetSnapshot);

        // --- Skip empty partitions upfront — avoids spawning consumers for partitions with no data ---
        allPartitions.removeIf(tp -> endOffsets.getOrDefault(tp, 0L) == 0L);
        if (allPartitions.isEmpty()) {
            safeLog.accept("[Search] No data in any partition.");
            return SearchResult.success(Collections.emptyList());
        }

        int totalPartitions = allPartitions.size();
        final int perPartitionLimit = (maxResults / totalPartitions) + 25;

        int batchSize = specificPartition != null ? 1 : BATCH_SIZE;
        int totalBatches = (int) Math.ceil((double) totalPartitions / batchSize);

        safeLog.accept("[Search] Topic: " + topic + " | Partitions: " + totalPartitions
                + " | perPartitionLimit: " + perPartitionLimit
                + " | Threads: " + batchSize
                + " | Filter: " + (hasTextFilter ? "'" + searchText + "'" : "none")
                + " | Time: " + (fromMs != null ? "yes" : "no"));
        logger.info("Search: {} partition(s), perPartitionLimit={}, batchSize={}, fromMs={}, toMs={}",
                totalPartitions, perPartitionLimit, batchSize, fromMs, toMs);

        // --- Step 3: shared collectors — plain ArrayList + synchronized (faster than CopyOnWriteArrayList) ---
        final List<String> allResults = new ArrayList<>();
        final List<long[]> allTimestamps = new ArrayList<>();
        final Object resultLock = new Object();
        AtomicInteger totalScanned = new AtomicInteger(0);
        AtomicInteger lastLoggedAt = new AtomicInteger(0);
        AtomicInteger resultCount = new AtomicInteger(0); // global cap — stops collecting once maxResults reached

        // --- Step 4: process partitions with reusable thread pool ---
        ExecutorService executor = Executors.newFixedThreadPool(batchSize);
        CountDownLatch latch = new CountDownLatch(totalPartitions);

        for (int partitionIdx = 0; partitionIdx < totalPartitions; partitionIdx++) {
            final int idx = partitionIdx;
            TopicPartition tp = allPartitions.get(partitionIdx);

            executor.submit(() -> {
                try {
                    if (stopFlag != null && stopFlag.get()) return;
                    safeLog.accept("[Search] Starting partition " + tp.partition() + " (" + (idx + 1) + "/" + totalPartitions + ")");

                    Properties props = buildConsumerProps(bootstrapServers, resolvedKeyDeser, resolvedValDeser, topic);
                    try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props)) {
                        consumer.assign(Collections.singletonList(tp));

                        // --- Seek strategy ---
                        if (specificPartition != null && specificOffset != null) {
                            consumer.seek(tp, specificOffset);
                        } else if (fromMs != null) {
                            Map<TopicPartition, OffsetAndTimestamp> tsOffsets =
                                    consumer.offsetsForTimes(Collections.singletonMap(tp, fromMs));
                            OffsetAndTimestamp ots = tsOffsets.get(tp);
                            if (ots != null) consumer.seek(tp, ots.offset());
                            else return; // no data after fromMs
                        } else if (fromBeginning) {
                            consumer.seek(tp, beginOffsets.getOrDefault(tp, 0L));
                        } else if (hasTextFilter) {
                            long end = endOffsets.getOrDefault(tp, 0L);
                            long begin = beginOffsets.getOrDefault(tp, 0L);
                            consumer.seek(tp, Math.max(begin, end - TAIL_SCAN_WINDOW));
                        } else {
                            long end = endOffsets.getOrDefault(tp, 0L);
                            long begin = beginOffsets.getOrDefault(tp, 0L);
                            consumer.seek(tp, Math.max(begin, end - perPartitionLimit));
                        }

                        long partitionEnd = endOffsets.getOrDefault(tp, 0L);
                        if (partitionEnd == 0) return; // empty partition

                        int emptyPolls = 0;
                        outer:
                        while (emptyPolls < 5) {
                            if (stopFlag != null && stopFlag.get()) break;
                            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(2));
                            if (records.isEmpty()) { emptyPolls++; continue; }
                            emptyPolls = 0;

                            for (ConsumerRecord<String, Object> rec : records) {
                                if (stopFlag != null && stopFlag.get()) break outer;
                                if (toMs != null && rec.timestamp() > toMs) break outer;

                                // Offset-targeted fetch: only the record at the requested offset.
                                if (specificOffset != null && rec.offset() < specificOffset) continue;

                                // Global cap: stop collecting once maxResults is reached across all partitions
                                if (resultCount.get() >= maxResults) break outer;

                                int scanned = totalScanned.incrementAndGet();
                                int lastLogged = lastLoggedAt.get();
                                if (scanned - lastLogged >= 500
                                        && lastLoggedAt.compareAndSet(lastLogged, scanned)) {
                                    safeLog.accept("[Search] Scanned " + scanned + " messages, "
                                            + resultCount.get() + " match(es) so far...");
                                }

                                // Only call formatValue() when needed:
                                // - text filter: need string to match against
                                // - no filter: matches unconditionally, defer formatValue() to result build
                                boolean matches;
                                String valueStr = null;
                                if (specificOffset != null) {
                                    matches = true; // offset-targeted fetch returns that exact message regardless of filter
                                } else if (hasTextFilter) {
                                    valueStr = formatValue(rec.value());
                                    matches = matchesFilter(valueStr, searchText);
                                } else {
                                    matches = true; // no filter — every record matches
                                }

                                if (matches) {
                                    int collected = resultCount.incrementAndGet();
                                    if (collected > maxResults) break outer; // another thread beat us
                                    if (valueStr == null) valueStr = formatValue(rec.value()); // lazy format
                                    safeLog.accept("[Search] Found match at partition "
                                            + rec.partition() + ", offset " + rec.offset());

                                    // Build JSON object for the result
                                    Map<String, Object> resultMap = new LinkedHashMap<>();
                                    resultMap.put("timestampEpochMs", rec.timestamp());
                                    resultMap.put("timestampType", rec.timestampType() != null ? rec.timestampType().name() : "UNKNOWN");
                                    resultMap.put("partition", rec.partition());
                                    resultMap.put("offset", rec.offset());
                                    resultMap.put("key", rec.key());

                                    String headersStr = formatHeaders(rec.headers());
                                    resultMap.put("headers", headersStr.isEmpty() ? null : parseHeadersToMap(rec.headers()));
                                    // Parse value as JSON object if possible for proper formatting
                                    resultMap.put("value", parseValueToJson(valueStr));

                                    String jsonOutput;
                                    try {
                                        jsonOutput = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(resultMap);
                                    } catch (Exception e) {
                                        jsonOutput = "{\"error\": \"Failed to format as JSON\"}";
                                    }

                                    synchronized (resultLock) {
                                        allResults.add(jsonOutput);
                                        allTimestamps.add(new long[]{rec.timestamp()});
                                    }
                                }
                                // When a specific offset was requested, return only that single message.
                                if (specificOffset != null) break outer;
                                if (rec.offset() >= partitionEnd - 1) break outer;
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Search error on partition {}: {}", tp.partition(), e.getMessage());
                    safeLog.accept("[Search] WARN partition " + tp.partition() + ": " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all partitions to complete
        executor.shutdown();
        try {
            if (!latch.await(120 * totalBatches, TimeUnit.SECONDS)) {
                safeLog.accept("[Search] WARN: some partitions timed out");
                logger.warn("Latch timed out after waiting for all partitions");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            safeLog.accept("[Search] Interrupted");
        } finally {
            executor.shutdownNow(); // ensure all threads released
        }

        boolean stopped = stopFlag != null && stopFlag.get();
        if (stopped) {
            safeLog.accept("[Search] Stopped by user. " + allResults.size() + " result(s) collected so far.");
        }

        // --- Step 5: sort by timestamp descending, cap at maxResults ---
        int size = allResults.size();
        Integer[] indices = new Integer[size];
        for (int i = 0; i < size; i++) indices[i] = i;
        Arrays.sort(indices, (a, b) -> Long.compare(allTimestamps.get(b)[0], allTimestamps.get(a)[0]));

        List<String> sorted = new ArrayList<>(Math.min(size, maxResults));
        for (int i = 0; i < Math.min(size, maxResults); i++) {
            sorted.add(allResults.get(indices[i]));
        }
        // Release source collections — sorted has the only copy we need from here on
        allResults.clear();
        allTimestamps.clear();

        logger.info("Search complete: {} result(s) from {} total across {} partition(s) (stopped={})",
                sorted.size(), size, totalPartitions, stopped);
        safeLog.accept("[Search] Complete: " + sorted.size() + " result(s) from "
                + totalScanned.get() + " scanned across " + totalPartitions + " partition(s)"
                + (stopped ? " [stopped]" : ""));
        return SearchResult.success(sorted);
    }

    private Properties buildConsumerProps(String bootstrapServers, String keyDeser, String valDeser, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "search-" + topic + "-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeser);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, resolveValueDeserializer(valDeser));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "200");
        if (customClassLoader != null) {
            Thread.currentThread().setContextClassLoader(customClassLoader);
        }
        return props;
    }

    // --- Helper methods ---

    private String resolveDeserializer(String input) {
        return (input == null || input.trim().isEmpty()) ? DEFAULT_DESERIALIZER : input.trim();
    }

    private String resolveValueDeserializer(String input) {
        if (decodeAsFlatbuf) return "org.apache.kafka.common.serialization.ByteArrayDeserializer";
        return resolveDeserializer(input);
    }

    private boolean matchesFilter(String value, String filter) {
        if (filter == null || filter.trim().isEmpty()) return true;
        String valueLower = value.toLowerCase();
        if (filter.contains(" AND ")) {
            for (String term : filter.split(" AND ", -1)) {
                if (!valueLower.contains(term.trim().toLowerCase())) return false;
            }
            return true;
        } else if (filter.contains(" OR ")) {
            for (String term : filter.split(" OR ", -1)) {
                if (valueLower.contains(term.trim().toLowerCase())) return true;
            }
            return false;
        }
        return valueLower.contains(filter.trim().toLowerCase());
    }

    private String formatValue(Object val) {
        if (val instanceof Message) {
            try {
                return JsonFormat.printer().preservingProtoFieldNames().print((Message) val);
            } catch (Exception e) {
                // Fallback: avoid val.toString() which may also trigger protobuf serialization errors
                // due to version mismatch between protobuf runtime and generated classes
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
            // Try to parse as JSON and pretty-print if valid
            try {
                Object json = objectMapper.readValue(str, Object.class);
                return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
            } catch (Exception e) {
                // Not valid JSON, return as-is
                return str;
            }
        }
        return val == null ? "null" : val.toString();
    }

    private String formatFlatBufferBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "(empty)";

        // Diagnostic: log first 32 bytes as hex and attempt UTF-8 prefix to identify wrapping
        if (logger.isDebugEnabled() || true) {
            int preview = Math.min(bytes.length, 32);
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < preview; i++) hex.append(String.format("%02x ", bytes[i]));
            String utf8Prefix = "";
            try { utf8Prefix = new String(bytes, 0, Math.min(bytes.length, 64), java.nio.charset.StandardCharsets.UTF_8); } catch (Exception ignored) {}
            logger.info("[FlatBuf] bytes.length={}, first32hex=[{}], utf8prefix=[{}]", bytes.length, hex.toString().trim(), utf8Prefix.replaceAll("\\p{C}", "?"));
        }

        // Try stripping a possible length-prefix (4-byte int header used by some Kafka serializers)
        if (bytes.length > 4) {
            java.nio.ByteBuffer probe = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int possibleLen = probe.getInt(0);
            if (possibleLen > 0 && possibleLen == bytes.length - 4) {
                logger.info("[FlatBuf] Detected 4-byte length prefix ({}), stripping and retrying", possibleLen);
                byte[] stripped = java.util.Arrays.copyOfRange(bytes, 4, bytes.length);
                String decoded = tryDecodeFlatBuffer(stripped, customClassLoader);
                if (decoded != null) return decoded;
            }
        }

        String decoded = tryDecodeFlatBuffer(bytes, customClassLoader);
        if (decoded != null) return decoded;

        // Not a decodable FlatBuffer — the bytes may actually be plain UTF-8 JSON/text
        // (e.g. a message that was pushed as JSON). Render it readably instead of hex.
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

    /**
     * Attempts to interpret raw bytes as a UTF-8 JSON document and pretty-print it.
     * Returns null if the bytes are not valid JSON text. Used as a graceful fallback
     * when FlatBuffer decoding fails but the payload is actually plain JSON.
     */
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
            java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);

            // If a class name was explicitly provided, try it first
            if (activeFlatbufClassName != null) {
                try {
                    ClassLoader loader = cl != null ? cl : Thread.currentThread().getContextClassLoader();
                    Class<?> clazz = Class.forName(activeFlatbufClassName, true, loader);
                    // First try native object approach
                    Object table = tryGetRootObject(clazz, buf.duplicate());
                    if (table != null) {
                        String json = flatTableToJson(table, clazz);
                        // Validate — if all values are null/0 it means Table.bb was not set (version mismatch)
                        if (!isEmptyResult(json)) return json;
                        logger.warn("[FlatBuf] getRootAs produced all-null result for {} — falling back to direct byte parser (likely flatbuffers-java version mismatch)", clazz.getSimpleName());
                    }
                    // Fall back: parse FlatBuffer bytes directly using field names from the generated class
                    String json = parseFlatBufferDirect(bytes, clazz);
                    if (json != null) return json;
                } catch (Exception e) {
                    logger.warn("Could not decode FlatBuffer with class '{}': {}", activeFlatbufClassName, e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.debug("FlatBuffer decode attempt failed: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Checks if a flatTableToJson result has all null/zero/false values — indicating a version mismatch
     * where the object was created but its ByteBuffer was never populated.
     */
    private boolean isEmptyResult(String json) {
        if (json == null) return true;
        // Strip whitespace, keys, and structural chars — if only null/0/false remain, it's empty
        String stripped = json.replaceAll("\"[^\"]+\"\\s*:\\s*", "")  // remove keys
                              .replaceAll("[{}\\[\\],\\s]", "");       // remove structure
        // Non-empty result has at least one non-null/non-zero string or number > 0
        return stripped.replaceAll("null|false|true|0", "").isEmpty();
    }

    /**
     * Parses a FlatBuffers binary directly at the byte level.
     * Extracts the exact vtable slot for each field accessor by reading the
     * integer constant passed to __offset(N) from the class bytecode — this
     * gives the correct slot regardless of method declaration order.
     * Works with any flatbuffers-java compiler version and bypasses runtime
     * version mismatch issues entirely.
     */
    private String parseFlatBufferDirect(byte[] bytes, Class<?> clazz) {
        try {
            java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);

            // Read root object position
            int rootOffset = bb.getInt(0);
            // vtable is at rootOffset - vtableRelOffset
            int vtableOffset = rootOffset - bb.getInt(rootOffset);
            int vtableSize = bb.getShort(vtableOffset) & 0xFFFF;

            // Build slot→(fieldName, returnType) map by scanning class bytecode
            Map<Integer, java.lang.reflect.Method> slotToMethod = buildSlotMap(clazz);
            if (slotToMethod.isEmpty()) {
                logger.warn("[FlatBuf] Could not extract slot map from bytecode for {}", clazz.getSimpleName());
                return null;
            }

            Map<String, Object> map = new LinkedHashMap<>();

            for (Map.Entry<Integer, java.lang.reflect.Method> entry : slotToMethod.entrySet()) {
                int slot = entry.getKey();
                java.lang.reflect.Method m = entry.getValue();
                if (slot >= vtableSize) {
                    map.put(m.getName(), null);
                    continue;
                }
                int fieldOffset = bb.getShort(vtableOffset + slot) & 0xFFFF;
                Class<?> ret = m.getReturnType();
                if (fieldOffset == 0) {
                    if (ret == boolean.class) map.put(m.getName(), false);
                    else if (ret == long.class || ret == int.class || ret == short.class || ret == byte.class) map.put(m.getName(), 0);
                    else map.put(m.getName(), null);
                    continue;
                }
                int abs = rootOffset + fieldOffset;
                try {
                    if (ret == String.class) {
                        int strAbs = abs + bb.getInt(abs);
                        int strLen = bb.getInt(strAbs);
                        byte[] strBytes = new byte[strLen];
                        bb.position(strAbs + 4);
                        bb.get(strBytes);
                        map.put(m.getName(), new String(strBytes, java.nio.charset.StandardCharsets.UTF_8));
                    } else if (ret == long.class)    { map.put(m.getName(), bb.getLong(abs)); }
                    else if (ret == int.class)        { map.put(m.getName(), bb.getInt(abs)); }
                    else if (ret == short.class)      { map.put(m.getName(), (int) bb.getShort(abs)); }
                    else if (ret == byte.class)       { map.put(m.getName(), (int) bb.get(abs)); }
                    else if (ret == boolean.class)    { map.put(m.getName(), bb.get(abs) != 0); }
                    else if (ret == float.class)      { map.put(m.getName(), bb.getFloat(abs)); }
                    else if (ret == double.class)     { map.put(m.getName(), bb.getDouble(abs)); }
                    // vectors (String[] / Table[]) — read length only for now
                    else if (ret == int.class)        { map.put(m.getName(), bb.getInt(abs)); }
                } catch (Exception e) {
                    map.put(m.getName(), null);
                }
            }

            if (map.isEmpty()) return null;
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
        } catch (Exception e) {
            logger.debug("Direct FlatBuffer parse failed: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Reads the class bytecode for each no-arg field accessor method and extracts
     * the integer constant pushed before the __offset(N) call. This is the vtable
     * slot number used in the FlatBuffers binary format.
     * Returns a TreeMap keyed by slot so fields appear in schema order.
     */
    private Map<Integer, java.lang.reflect.Method> buildSlotMap(Class<?> clazz) {
        Map<Integer, java.lang.reflect.Method> slotMap = new java.util.TreeMap<>();
        // Build index of method names we care about
        Map<String, java.lang.reflect.Method> methodIndex = new java.util.HashMap<>();
        for (java.lang.reflect.Method m : clazz.getMethods()) {
            if (m.getParameterCount() != 0) continue;
            if (!m.getDeclaringClass().getName().equals(clazz.getName())) continue;
            if (m.getName().startsWith("__") || m.getName().startsWith("mutate")) continue;
            if (FLATBUF_EXCLUDED_METHODS.contains(m.getName())) continue;
            Class<?> ret = m.getReturnType();
            if (ret == void.class || java.nio.ByteBuffer.class.isAssignableFrom(ret)) continue;
            if (m.getName().endsWith("Length")) continue;
            // skip multi-param vector accessors like evidences(int j)
            methodIndex.put(m.getName(), m);
        }

        // Load the .class bytes
        String resourceName = clazz.getName().replace('.', '/') + ".class";
        byte[] classBytes = null;
        try (java.io.InputStream is = clazz.getClassLoader().getResourceAsStream(resourceName)) {
            if (is != null) classBytes = is.readAllBytes();
        } catch (Exception e) {
            logger.debug("Could not load class bytes for {}: {}", clazz.getName(), e.getMessage());
            return slotMap;
        }
        if (classBytes == null) return slotMap;

        // Minimal bytecode scan: find each method and the SIPUSH/BIPUSH/ICONST before __offset
        // We use a simple constant-pool + method scan without a full ASM dependency
        try {
            java.nio.ByteBuffer cb = java.nio.ByteBuffer.wrap(classBytes);
            cb.order(java.nio.ByteOrder.BIG_ENDIAN); // class file is big-endian
            cb.getInt(); // magic 0xCAFEBABE
            cb.getShort(); cb.getShort(); // minor, major
            int cpCount = cb.getShort() & 0xFFFF;
            // Parse constant pool to find UTF8 strings (method names)
            Object[] cp = new Object[cpCount];
            for (int i = 1; i < cpCount; i++) {
                int tag = cb.get() & 0xFF;
                switch (tag) {
                    case 1: { int len = cb.getShort() & 0xFFFF; byte[] s = new byte[len]; cb.get(s); cp[i] = new String(s, java.nio.charset.StandardCharsets.UTF_8); break; }
                    case 3: case 4: cb.getInt(); break;
                    case 5: case 6: cb.getLong(); i++; break; // long/double take two slots
                    case 7: case 8: case 16: case 19: case 20: cb.getShort(); break;
                    case 9: case 10: case 11: case 12: case 17: case 18: cb.getInt(); break;
                    case 15: cb.get(); cb.getShort(); break;
                    default: logger.debug("Unknown CP tag {} at index {}", tag, i); break;
                }
            }
            cb.getShort(); // access flags
            cb.getShort(); // this class
            cb.getShort(); // super class
            int ifCount = cb.getShort() & 0xFFFF;
            for (int i = 0; i < ifCount; i++) cb.getShort();
            int fieldCount = cb.getShort() & 0xFFFF;
            for (int i = 0; i < fieldCount; i++) {
                cb.getShort(); cb.getShort(); cb.getShort(); // access, name, desc
                int attrCount = cb.getShort() & 0xFFFF;
                for (int a = 0; a < attrCount; a++) { cb.getShort(); int len = cb.getInt(); cb.position(cb.position() + len); }
            }
            int methodCount = cb.getShort() & 0xFFFF;
            for (int mi = 0; mi < methodCount; mi++) {
                cb.getShort(); // access
                int nameIdx = cb.getShort() & 0xFFFF;
                cb.getShort(); // descriptor
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
                // Scan bytecode for BIPUSH(16)/SIPUSH(17)/ICONST_N before __offset invoke
                // Format: push <slot>, invokevirtual/__offset
                int slot = scanForOffsetSlot(codeBytes);
                if (slot > 0) {
                    slotMap.put(slot, methodIndex.get(mName));
                }
            }
        } catch (Exception e) {
            logger.debug("Bytecode slot scan failed for {}: {}", clazz.getSimpleName(), e.getMessage());
        }
        return slotMap;
    }

    /** Scans method bytecode for the integer constant pushed before an __offset call. */
    private int scanForOffsetSlot(byte[] code) {
        // Skip max_stack(2) + max_locals(2) + code_length(4) header
        if (code.length < 8) return -1;
        java.nio.ByteBuffer cb = java.nio.ByteBuffer.wrap(code);
        cb.getShort(); cb.getShort(); // max_stack, max_locals
        int codeLen = cb.getInt();
        int lastPushedInt = -1;
        int end = cb.position() + codeLen;
        while (cb.position() < end) {
            int op = cb.get() & 0xFF;
            if (op == 0x10) { lastPushedInt = cb.get(); } // BIPUSH
            else if (op == 0x11) { lastPushedInt = cb.getShort(); } // SIPUSH
            else if (op >= 0x02 && op <= 0x08) { lastPushedInt = op - 3; } // ICONST_M1..ICONST_5
            else if (op == 0xB6 || op == 0xB7 || op == 0xB8 || op == 0xB9) {
                // invokevirtual/special/static/interface — check if it's __offset
                int cpRef = cb.getShort() & 0xFFFF;
                // We just check: if a push preceded this invoke and value looks like a vtable slot
                if (lastPushedInt > 0 && lastPushedInt % 2 == 0 && lastPushedInt >= 4) {
                    return lastPushedInt; // first invoke after a push is __offset(N)
                }
                lastPushedInt = -1;
            } else {
                // skip operands for known multi-byte opcodes
                if (op == 0xC4) op = cb.get() & 0xFF; // wide prefix
                lastPushedInt = -1;
                int skip = OPCODE_SKIP[op];
                if (skip < 0) break; // unknown, stop
                cb.position(Math.min(cb.position() + skip, end));
            }
        }
        return -1;
    }

    private static final int[] OPCODE_SKIP = new int[256];
    static {
        java.util.Arrays.fill(OPCODE_SKIP, 0);
        // 1-byte operand opcodes
        for (int op : new int[]{0x10,0x12,0x15,0x16,0x17,0x18,0x19,0x36,0x37,0x38,0x39,0x3A,0xA9,0xBC}) OPCODE_SKIP[op] = 1;
        // 2-byte operand opcodes
        for (int op : new int[]{0x11,0x13,0x14,0x84,0xB2,0xB3,0xB4,0xB5,0xB6,0xB7,0xB8,0xBB,0xBD,0xC0,0xC1}) OPCODE_SKIP[op] = 2;
        // 4-byte operand
        for (int op : new int[]{0xB9,0xBA}) OPCODE_SKIP[op] = 4;
        // branch opcodes (2-byte)
        for (int op : new int[]{0x99,0x9A,0x9B,0x9C,0x9D,0x9E,0x9F,0xA0,0xA1,0xA2,0xA3,0xA4,0xA5,0xA6,0xA7,0xC6,0xC7}) OPCODE_SKIP[op] = 2;
        for (int op : new int[]{0xC8,0xC9}) OPCODE_SKIP[op] = 4; // goto_w, jsr_w
        OPCODE_SKIP[0xAA] = -1; OPCODE_SKIP[0xAB] = -1; // tableswitch, lookupswitch — complex, stop
        OPCODE_SKIP[0xC4] = 0; // wide — handled inline
    }

    /**
     * Tries multiple strategies to obtain a root object from a FlatBuffer ByteBuffer:
     * 1. Standard generated: getRootAs<ClassName>(ByteBuffer)
     * 2. Any static method on the class that takes a single ByteBuffer and returns an instance of the class
     * 3. No-arg constructor + __init(int, ByteBuffer) (low-level FlatBuffers Table init)
     */
    private Object tryGetRootObject(Class<?> clazz, java.nio.ByteBuffer buf) {
        // Strategy 1: standard generated getRootAs<ClassName>(ByteBuffer)
        try {
            java.lang.reflect.Method m = clazz.getMethod("getRootAs" + clazz.getSimpleName(), java.nio.ByteBuffer.class);
            Object result = m.invoke(null, buf);
            if (result != null) return result;
        } catch (NoSuchMethodException e) {
            logger.warn("No getRootAs{} method on {} — trying other strategies", clazz.getSimpleName(), clazz.getName());
        } catch (java.lang.reflect.InvocationTargetException e) {
            logger.warn("getRootAs{} threw exception: {} — possible flatbuffers runtime version mismatch (class compiled against different flatbuffers-java version)", clazz.getSimpleName(), e.getCause() != null ? e.getCause().toString() : e.toString());
        } catch (Exception e) {
            logger.warn("getRootAs strategy failed for {}: {}", clazz.getSimpleName(), e.toString());
        }

        // Strategy 2: any static method returning this class and taking only a ByteBuffer
        for (java.lang.reflect.Method m : clazz.getMethods()) {
            if (!java.lang.reflect.Modifier.isStatic(m.getModifiers())) continue;
            if (m.getParameterCount() != 1) continue;
            if (!m.getParameterTypes()[0].equals(java.nio.ByteBuffer.class)) continue;
            if (!clazz.isAssignableFrom(m.getReturnType())) continue;
            try {
                logger.debug("Trying static factory method: {}#{}", clazz.getSimpleName(), m.getName());
                Object result = m.invoke(null, buf);
                if (result != null) return result;
            } catch (Exception e) {
                logger.warn("Static factory {}#{} failed: {}", clazz.getSimpleName(), m.getName(), e.toString());
            }
        }

        // Strategy 3: no-arg constructor + __init(int offset, ByteBuffer) — raw Table init
        try {
            java.lang.reflect.Constructor<?> ctor = clazz.getDeclaredConstructor();
            ctor.setAccessible(true);
            Object instance = ctor.newInstance();
            buf.order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int rootOffset = buf.getInt(0);
            java.lang.reflect.Method init = clazz.getMethod("__init", int.class, java.nio.ByteBuffer.class);
            logger.debug("Trying __init strategy for {} at rootOffset={}", clazz.getSimpleName(), rootOffset);
            init.invoke(instance, rootOffset, buf);
            return instance;
        } catch (Exception e) {
            logger.warn("__init strategy failed for {}: {}", clazz.getSimpleName(), e.toString());
        }

        logger.warn("All decode strategies exhausted for class {}. " +
                "If you see 'IncompatibleClassChangeError' or 'NoSuchMethodError', the schema JAR was compiled against a different " +
                "flatbuffers-java version than the one on the classpath ({}). " +
                "Load the matching flatbuffers-java JAR alongside the schema JAR.",
                clazz.getName(), com.google.flatbuffers.Table.class.getPackage().getImplementationVersion());
        return null;
    }

    private List<Class<?>> discoverFlatBufferClasses(ClassLoader cl) {
        List<Class<?>> results = new ArrayList<>();
        try {
            if (cl instanceof java.net.URLClassLoader) {
                java.net.URL[] urls = ((java.net.URLClassLoader) cl).getURLs();
                for (java.net.URL url : urls) {
                    try (java.util.jar.JarFile jar = new java.util.jar.JarFile(url.getFile())) {
                        java.util.Enumeration<java.util.jar.JarEntry> entries = jar.entries();
                        while (entries.hasMoreElements()) {
                            String name = entries.nextElement().getName();
                            if (name.endsWith(".class")) {
                                String className = name.replace('/', '.').replace(".class", "");
                                try {
                                    Class<?> c = cl.loadClass(className);
                                    if (com.google.flatbuffers.Table.class.isAssignableFrom(c)) {
                                        results.add(c);
                                    }
                                } catch (Throwable ignored) {
                                }
                            }
                        }
                    } catch (Exception ignored) {
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Could not scan classloader for FlatBuffer classes: {}", e.getMessage());
        }
        return results;
    }

    private static final java.util.Set<String> FLATBUF_EXCLUDED_METHODS = new java.util.HashSet<>(java.util.Arrays.asList(
            "getClass", "hashCode", "toString", "wait", "notify", "notifyAll",
            "keysVector", "valuesVector"
    ));

    private String flatTableToJson(Object table, Class<?> clazz) {
        // Always use the actual runtime class of the object — handles classloader mismatch
        Class<?> actualClass = table.getClass();
        String topLevelClassName = actualClass.getName();
        Map<String, Object> map = new LinkedHashMap<>();
        for (java.lang.reflect.Method m : actualClass.getMethods()) {
            if (m.getParameterCount() != 0) continue;
            if (FLATBUF_EXCLUDED_METHODS.contains(m.getName())) continue;
            if (m.getName().startsWith("__")) continue;
            if (m.getName().startsWith("mutate")) continue;
            // Only include methods declared on this class (not Object/Table superclass methods)
            if (!m.getDeclaringClass().getName().equals(topLevelClassName)) continue;
            // Skip void and ByteBuffer return types
            Class<?> ret = m.getReturnType();
            if (ret == void.class) continue;
            if (java.nio.ByteBuffer.class.isAssignableFrom(ret)) continue;
            try {
                Object v = m.invoke(table);
                // Recurse into nested objects that look like FlatBuffer Tables
                if (v != null) {
                    boolean isNestedTable = false;
                    for (Class<?> iface : v.getClass().getSuperclass() != null
                            ? new Class[]{v.getClass(), v.getClass().getSuperclass()} : new Class[]{v.getClass()}) {
                        if (iface.getName().equals("com.google.flatbuffers.Table")) { isNestedTable = true; break; }
                    }
                    if (!isNestedTable) {
                        // also check by superclass chain name
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
                map.put(m.getName(), v);
            } catch (Exception ignored) {
            }
        }
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
        } catch (Exception e) {
            return map.toString();
        }
    }

    private int findPartitionWithMostMessages(KafkaConsumer<String, Object> consumer,
                                              String topic, List<PartitionInfo> partitions) {
        int bestPartition = 0;
        long maxMessages = 0;

        for (PartitionInfo pi : partitions) {
            TopicPartition tp = new TopicPartition(topic, pi.partition());
            try {
                long begin = consumer.beginningOffsets(Collections.singleton(tp)).get(tp);
                long end = consumer.endOffsets(Collections.singleton(tp)).get(tp);
                long count = end - begin;
                if (count > maxMessages) {
                    maxMessages = count;
                    bestPartition = pi.partition();
                }
            } catch (Exception e) {
                logger.warn("Could not get offset info for partition {}", pi.partition(), e);
            }
        }
        return bestPartition;
    }

    private int estimateSize(Object value) {
        if (value instanceof String) return ((String) value).length();
        if (value == null) return 0;
        try {
            Method sizeMethod = value.getClass().getMethod("getSerializedSize");
            return (Integer) sizeMethod.invoke(value);
        } catch (Exception e) {
            return value.toString().length();
        }
    }

    private String formatHeaders(org.apache.kafka.common.header.Headers headers) {
        if (headers == null) return "";
        StringBuilder sb = new StringBuilder();
        for (org.apache.kafka.common.header.Header h : headers) {
            if (sb.length() > 0) sb.append(", ");
            sb.append(h.key()).append("=").append(h.value() != null ? new String(h.value()) : "null");
        }
        return sb.toString();
    }

    private String convertProtobufToJson(Object protobufMessage) {
        try {
            if (protobufMessage instanceof com.google.protobuf.MessageLite) {
                String jsonString = JsonFormat.printer()
                        .includingDefaultValueFields()
                        .preservingProtoFieldNames()
                        .print((Message) protobufMessage);
                return jsonString;
            }
            return null;
        } catch (Exception e) {
            logger.error("Error converting Protobuf to JSON", e);
            return null;
        }
    }

    // --- Result DTOs ---

    public static class PeekResult {
        private final boolean success;
        private final String data;
        private final String status;
        private final String messageJson;

        private PeekResult(boolean success, String data, String status, String messageJson) {
            this.success = success;
            this.data = data;
            this.status = status;
            this.messageJson = messageJson;
        }

        public static PeekResult success(String data, String status, String messageJson) { return new PeekResult(true, data, status, messageJson); }
        public static PeekResult error(String message) { return new PeekResult(false, null, message, null); }

        public boolean isSuccess() { return success; }
        public String getData() { return data; }
        public String getStatus() { return status; }
        public String getMessageJson() { return messageJson; }
    }

    public static class SearchResult {
        private final boolean success;
        private final List<String> results;
        private final String error;
        private final Map<Integer, Long> partitionOffsets; // partition -> max offset found

        private SearchResult(boolean success, List<String> results, String error, Map<Integer, Long> partitionOffsets) {
            this.success = success;
            this.results = results;
            this.error = error;
            this.partitionOffsets = partitionOffsets != null ? partitionOffsets : new HashMap<>();
        }

        public static SearchResult success(List<String> results) {
            return new SearchResult(true, results, null, extractPartitionOffsets(results));
        }
        public static SearchResult error(String message) { return new SearchResult(false, Collections.emptyList(), message, null); }

        public boolean isSuccess() { return success; }
        public List<String> getResults() { return results; }
        public String getError() { return error; }
        public Map<Integer, Long> getPartitionOffsets() { return partitionOffsets; }

        /**
         * Extracts partition -> max offset mapping from JSON results.
         * Format: partition=offset (one per line)
         */
        private static Map<Integer, Long> extractPartitionOffsets(List<String> results) {
            Map<Integer, Long> offsets = new HashMap<>();
            if (results == null || results.isEmpty()) return offsets;

            for (String json : results) {
                try {
                    Map<?, ?> map = objectMapper.readValue(json, Map.class);
                    Object partitionObj = map.get("partition");
                    Object offsetObj = map.get("offset");
                    if (partitionObj instanceof Number && offsetObj instanceof Number) {
                        int partition = ((Number) partitionObj).intValue();
                        long offset = ((Number) offsetObj).longValue();
                        // Keep the maximum offset per partition
                        offsets.merge(partition, offset, Math::max);
                    }
                } catch (Exception e) {
                    // Skip malformed results
                    logger.warn("Failed to extract offset from result: {}", e.getMessage());
                }
            }
            return offsets;
        }
    }
}
