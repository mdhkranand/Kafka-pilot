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
        valueDeserializer = resolveDeserializer(valueDeserializer);

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
        String json = (rec.value() instanceof String) ? (String) rec.value() : convertProtobufToJson(rec.value());
        if (json == null) json = formatValue(rec.value());

        String ts = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(rec.timestamp()), ZoneId.systemDefault()).format(DISPLAY_FORMAT);

        String headers = formatHeaders(rec.headers());

        StringBuilder body = new StringBuilder();
        body.append("=== Message Details ===\n");
        body.append("Timestamp: ").append(ts).append("\n");
        body.append("Partition: ").append(rec.partition()).append("\n");
        body.append("Offset: ").append(rec.offset()).append("\n");
        body.append("Key: ").append(rec.key() != null ? rec.key() : "null").append("\n");
        if (!headers.isEmpty()) {
            body.append("Headers: ").append(headers).append("\n");
        }
        body.append("\n=== Value ===\n").append(json);

        String status = "1 message from partition " + partition
                + (offset != null ? " @ offset " + offset : " (latest)");
        return PeekResult.success(body.toString(), status, json);
    }

    private static final int BATCH_SIZE = 10;        // partitions per batch — controls peak memory
    private static final long TAIL_SCAN_WINDOW = 1000L; // messages per partition for text-only scan

    /**
     * Searches messages across partitions in batches of BATCH_SIZE (10).
     * Each batch runs its own ExecutorService + CountDownLatch and is fully
     * torn down before the next batch starts — keeping peak memory bounded to
     * BATCH_SIZE concurrent KafkaConsumer instances at any time.
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
        final String resolvedValDeser = resolveDeserializer(valueDeserializer);
        final boolean hasTextFilter = searchText != null && !searchText.trim().isEmpty();
        Consumer<String> safeLog = msg -> { if (logConsumer != null) logConsumer.accept(msg); };

        // --- Step 1: discover all partitions ---
        List<TopicPartition> allPartitions = new ArrayList<>();
        try {
            Properties adminProps = buildConsumerProps(bootstrapServers, resolvedKeyDeser, resolvedValDeser, topic);
            try (KafkaConsumer<String, Object> adminConsumer = new KafkaConsumer<>(adminProps)) {
                if (specificPartition != null) {
                    allPartitions.add(new TopicPartition(topic, specificPartition));
                } else {
                    for (PartitionInfo pi : adminConsumer.partitionsFor(topic)) {
                        allPartitions.add(new TopicPartition(topic, pi.partition()));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get partitions for topic: {}", topic, e);
            safeLog.accept("[Search] ERROR getting partitions: " + e.getMessage());
            return SearchResult.error(e.getMessage());
        }

        // --- Step 2: snapshot end offsets once (stable reference, avoids live-topic drift) ---
        Map<TopicPartition, Long> endOffsetSnapshot = new HashMap<>();
        try {
            Properties adminProps = buildConsumerProps(bootstrapServers, resolvedKeyDeser, resolvedValDeser, topic);
            try (KafkaConsumer<String, Object> offsetConsumer = new KafkaConsumer<>(adminProps)) {
                offsetConsumer.assign(allPartitions);
                endOffsetSnapshot.putAll(offsetConsumer.endOffsets(allPartitions));
            }
        } catch (Exception e) {
            safeLog.accept("[Search] WARN: could not snapshot end offsets: " + e.getMessage());
        }
        final Map<TopicPartition, Long> endOffsets = Collections.unmodifiableMap(endOffsetSnapshot);

        int totalPartitions = allPartitions.size();
        int batchSize = specificPartition != null ? 1 : BATCH_SIZE;
        int totalBatches = (int) Math.ceil((double) totalPartitions / batchSize);

        safeLog.accept("[Search] Topic: " + topic + " | Partitions: " + totalPartitions
                + " | Batch size: " + batchSize + " | Batches: " + totalBatches
                + " | Filter: " + (hasTextFilter ? "'" + searchText + "'" : "none")
                + " | Time: " + (fromMs != null ? "yes" : "no"));
        logger.info("Search: {} partition(s), batchSize={}, batches={}, fromMs={}, toMs={}",
                totalPartitions, batchSize, totalBatches, fromMs, toMs);

        // --- Step 3: shared collectors — plain ArrayList + synchronized (faster than CopyOnWriteArrayList) ---
        final List<String> allResults = new ArrayList<>();
        final List<long[]> allTimestamps = new ArrayList<>();
        final Object resultLock = new Object();
        AtomicInteger totalScanned = new AtomicInteger(0);
        AtomicInteger lastLoggedAt = new AtomicInteger(0);
        AtomicInteger resultCount = new AtomicInteger(0); // global cap — stops collecting once maxResults reached

        // --- Step 4: process partitions in batches ---
        for (int batchIdx = 0; batchIdx < totalBatches; batchIdx++) {
            if (stopFlag != null && stopFlag.get()) break;
            if (resultCount.get() >= maxResults) break; // global cap reached — skip remaining batches

            int from = batchIdx * batchSize;
            int to = Math.min(from + batchSize, totalPartitions);
            List<TopicPartition> batch = allPartitions.subList(from, to);

            safeLog.accept("[Search] Batch " + (batchIdx + 1) + "/" + totalBatches
                    + " — partitions " + from + "-" + (to - 1));

            ExecutorService executor = Executors.newFixedThreadPool(batch.size());
            CountDownLatch latch = new CountDownLatch(batch.size());

            for (TopicPartition tp : batch) {
                executor.submit(() -> {
                    try {
                        if (stopFlag != null && stopFlag.get()) return;
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
                                else return; // no data after fromMs — outer finally releases latch
                            } else if (fromBeginning) {
                                long begin = consumer.beginningOffsets(Collections.singletonList(tp)).getOrDefault(tp, 0L);
                                consumer.seek(tp, begin);
                            } else if (hasTextFilter) {
                                long end = endOffsets.getOrDefault(tp, 0L);
                                long begin = consumer.beginningOffsets(Collections.singletonList(tp)).getOrDefault(tp, 0L);
                                consumer.seek(tp, Math.max(begin, end - TAIL_SCAN_WINDOW));
                            } else {
                                long end = endOffsets.getOrDefault(tp, 0L);
                                long begin = consumer.beginningOffsets(Collections.singletonList(tp)).getOrDefault(tp, 0L);
                                consumer.seek(tp, Math.max(begin, end - maxResults));
                            }

                            long partitionEnd = endOffsets.getOrDefault(tp, 0L);
                            if (partitionEnd == 0) return; // empty partition

                            int emptyPolls = 0;
                            outer:
                            while (emptyPolls < 5) {
                                if (stopFlag != null && stopFlag.get()) break;
                                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(5));
                                if (records.isEmpty()) { emptyPolls++; continue; }
                                emptyPolls = 0;

                                for (ConsumerRecord<String, Object> rec : records) {
                                    if (stopFlag != null && stopFlag.get()) break outer;
                                    if (toMs != null && rec.timestamp() > toMs) break outer;

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
                                    if (hasTextFilter) {
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
                                        String ts = LocalDateTime.ofInstant(
                                                Instant.ofEpochMilli(rec.timestamp()),
                                                ZoneId.systemDefault()).format(DISPLAY_FORMAT);
                                        String tsType = rec.timestampType() != null
                                                ? rec.timestampType().name : "UNKNOWN";
                                        String headers = formatHeaders(rec.headers());
                                        StringBuilder sb = new StringBuilder();
                                        sb.append("=== Message Details ===\n");
                                        sb.append("Timestamp: ").append(ts).append("  [").append(tsType).append("]\n");
                                        sb.append("Partition: ").append(rec.partition()).append("\n");
                                        sb.append("Offset:    ").append(rec.offset()).append("\n");
                                        sb.append("Key:       ").append(rec.key() != null ? rec.key() : "null").append("\n");
                                        if (!headers.isEmpty()) sb.append("Headers:   ").append(headers).append("\n");
                                        sb.append("\n=== Value ===\n").append(valueStr);
                                        synchronized (resultLock) {
                                            allResults.add(sb.toString());
                                            allTimestamps.add(new long[]{rec.timestamp()});
                                        }
                                    }
                                    if (rec.offset() >= partitionEnd - 1) break outer;
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("Search error on partition {}: {}", tp.partition(), e.getMessage());
                        safeLog.accept("[Search] WARN partition " + tp.partition() + ": " + e.getMessage());
                    } finally {
                        latch.countDown(); // always release
                    }
                });
            }

            // Wait for this batch, then tear down its executor before starting next batch
            executor.shutdown();
            try {
                if (!latch.await(120, TimeUnit.SECONDS)) {
                    safeLog.accept("[Search] WARN: batch " + (batchIdx + 1) + " timed out — continuing");
                    logger.warn("Batch {} latch timed out", batchIdx + 1);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                safeLog.accept("[Search] Interrupted");
                break;
            } finally {
                executor.shutdownNow(); // ensure all threads released before next batch
            }
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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valDeser);
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

        private SearchResult(boolean success, List<String> results, String error) {
            this.success = success;
            this.results = results;
            this.error = error;
        }

        public static SearchResult success(List<String> results) { return new SearchResult(true, results, null); }
        public static SearchResult error(String message) { return new SearchResult(false, Collections.emptyList(), message); }

        public boolean isSuccess() { return success; }
        public List<String> getResults() { return results; }
        public String getError() { return error; }
    }
}
