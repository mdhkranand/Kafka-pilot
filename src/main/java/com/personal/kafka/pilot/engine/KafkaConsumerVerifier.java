package com.personal.kafka.pilot.engine;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class KafkaConsumerVerifier {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerVerifier.class);

    public static class PartitionTarget {
        public final int partition;
        public final long targetOffset;

        public PartitionTarget(int partition, long targetOffset) {
            this.partition = partition;
            this.targetOffset = targetOffset;
        }
    }

    private static final long DEFAULT_TIMEOUT_MS = 10 * 60 * 1000L;

    private final String bootstrapServers;
    private final String topic;
    private final String consumerGroup;
    private final List<PartitionTarget> targets;
    private final Consumer<String> log;
    private final long timeoutMs;

    private volatile boolean shouldStop = false;
    private volatile boolean running = false;

    public KafkaConsumerVerifier(String bootstrapServers,
                                  String topic,
                                  String consumerGroup,
                                  List<PartitionTarget> targets,
                                  Consumer<String> log,
                                  long timeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.targets = targets;
        this.log = log;
        this.timeoutMs = timeoutMs > 0 ? timeoutMs : DEFAULT_TIMEOUT_MS;
    }

    public void stop() {
        shouldStop = true;
    }

    public boolean isRunning() {
        return running;
    }

    public void run() {
        shouldStop = false;
        running = true;

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", "15000");

        log("=== Consumer Verification Started ===");
        log("Topic         : " + topic);
        log("Consumer Group: " + consumerGroup);
        log("Tracking " + targets.size() + " partition(s)");
        log("Timeout       : " + (timeoutMs / 60000) + " minute(s)");
        log("");

        long verifyStartTime = System.currentTimeMillis();

        try (AdminClient admin = AdminClient.create(props)) {

            Map<Integer, Long> previousCommitted = new HashMap<>();
            Map<Integer, Long> rateStartCommitted = new HashMap<>();
            long rateStartTime = System.currentTimeMillis();

            Set<Integer> done = new HashSet<>();

            while (!shouldStop) {

                if (System.currentTimeMillis() - verifyStartTime >= timeoutMs) {
                    long totalMs = System.currentTimeMillis() - verifyStartTime;
                    log("⏰ TIMEOUT — verification did not complete within " + (timeoutMs / 60000) + " minute(s).");
                    log("Elapsed: " + formatDuration(totalMs));
                    break;
                }

                long pollStart = System.currentTimeMillis();

                Map<TopicPartition, Long> committedOffsets = fetchCommittedOffsets(admin);
                Map<TopicPartition, Long> logEndOffsets = fetchLogEndOffsets(admin);

                if (committedOffsets == null || logEndOffsets == null) {
                    log("ERROR: Could not fetch offsets, retrying...");
                    sleep(1000);
                    continue;
                }

                long now = System.currentTimeMillis();
                double elapsedRateSec = (now - rateStartTime) / 1000.0;

                log("─────────────────────────────────────────────────────────────────");
                log(String.format("%-10s %-14s %-14s %-10s %-14s %-10s",
                        "Partition", "Target Offset", "Committed", "Lag", "Rate (msg/s)", "Status"));
                log("─────────────────────────────────────────────────────────────────");

                for (PartitionTarget pt : targets) {
                    TopicPartition tp = new TopicPartition(topic, pt.partition);
                    long committed = committedOffsets.getOrDefault(tp, -1L);
                    long leo = logEndOffsets.getOrDefault(tp, -1L);

                    long lag = (committed >= 0 && pt.targetOffset > 0)
                            ? Math.max(0, pt.targetOffset - committed)
                            : (leo >= 0 && committed >= 0 ? leo - committed : -1);

                    double rate = 0.0;
                    if (elapsedRateSec > 0 && committed >= 0) {
                        long prevCommit = rateStartCommitted.getOrDefault(pt.partition, committed);
                        rate = (committed - prevCommit) / elapsedRateSec;
                    }

                    String status;
                    if (committed < 0) {
                        status = "⏳ No commit yet";
                    } else if (lag <= 0) {
                        status = "✓ DONE";
                        done.add(pt.partition);
                    } else {
                        status = "⏳ pending";
                    }

                    log(String.format("%-10d %-14d %-14d %-10d %-14s %-10s",
                            pt.partition, pt.targetOffset, committed,
                            Math.max(0, lag),
                            String.format("%.1f", rate),
                            status));

                    previousCommitted.put(pt.partition, committed);
                }

                boolean allDone = done.size() == targets.size();

                log("");

                if (allDone) {
                    long totalMs = System.currentTimeMillis() - verifyStartTime;
                    log("╔══════════════════════════════════════════════════════╗");
                    log("║           ALL MESSAGES CONSUMED                      ║");
                    log("╠══════════════════════════════════════════════════════╣");
                    log(String.format("║  Partitions verified : %-28d║", targets.size()));
                    log(String.format("║  Total time          : %-28s║", formatDuration(totalMs)));
                    log(String.format("║  Consumer group      : %-28s║", consumerGroup));
                    log("╚══════════════════════════════════════════════════════╝");
                    break;
                }

                if (elapsedRateSec >= 5.0) {
                    for (PartitionTarget pt : targets) {
                        TopicPartition tp = new TopicPartition(topic, pt.partition);
                        long committed = committedOffsets.getOrDefault(tp, -1L);
                        rateStartCommitted.put(pt.partition, committed);
                    }
                    rateStartTime = System.currentTimeMillis();
                }

                long elapsed = System.currentTimeMillis() - pollStart;
                long sleepMs = Math.max(0, 1000 - elapsed);
                sleep(sleepMs);
            }

            if (shouldStop) {
                long totalMs = System.currentTimeMillis() - verifyStartTime;
                log("Verification stopped after " + formatDuration(totalMs));
            }
            long remaining = timeoutMs - (System.currentTimeMillis() - verifyStartTime);
            if (remaining <= 0 && !shouldStop && done.size() < targets.size()) {
                log("⏰ Timed out — " + done.size() + "/" + targets.size() + " partition(s) completed.");
            }

        } catch (Exception e) {
            log("ERROR: " + e.getMessage());
            logger.error("Consumer verifier error", e);
        } finally {
            running = false;
            log("=== Verification Ended ===");
        }
    }

    private Map<TopicPartition, Long> fetchCommittedOffsets(AdminClient admin) {
        try {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionTarget pt : targets) tps.add(new TopicPartition(topic, pt.partition));

            ListConsumerGroupOffsetsOptions opts = new ListConsumerGroupOffsetsOptions()
                    .topicPartitions(tps);
            Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> result =
                    admin.listConsumerGroupOffsets(consumerGroup, opts)
                         .partitionsToOffsetAndMetadata()
                         .get(10, TimeUnit.SECONDS);

            Map<TopicPartition, Long> out = new HashMap<>();
            for (PartitionTarget pt : targets) {
                TopicPartition tp = new TopicPartition(topic, pt.partition);
                org.apache.kafka.clients.consumer.OffsetAndMetadata meta = result.get(tp);
                out.put(tp, meta != null ? meta.offset() : -1L);
            }
            return out;
        } catch (Exception e) {
            logger.warn("Failed to fetch committed offsets: {}", e.getMessage());
            return null;
        }
    }

    private Map<TopicPartition, Long> fetchLogEndOffsets(AdminClient admin) {
        try {
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            for (PartitionTarget pt : targets)
                request.put(new TopicPartition(topic, pt.partition), OffsetSpec.latest());

            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> result =
                    admin.listOffsets(request).all().get(10, TimeUnit.SECONDS);

            Map<TopicPartition, Long> out = new HashMap<>();
            result.forEach((tp, info) -> out.put(tp, info.offset()));
            return out;
        } catch (Exception e) {
            logger.warn("Failed to fetch log-end offsets: {}", e.getMessage());
            return null;
        }
    }

    private void log(String message) {
        log.accept(message);
        logger.info(message);
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    private String formatDuration(long ms) {
        long secs = ms / 1000;
        long mins = secs / 60;
        secs = secs % 60;
        if (mins > 0) return String.format("%dm %ds (%dms)", mins, secs, ms);
        return String.format("%ds %dms", secs, ms % 1000);
    }
}
