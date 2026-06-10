package com.personal.kafka.pilot.engine;

import com.google.common.collect.Lists;
import com.personal.kafka.pilot.model.TestConfiguration;
import com.personal.kafka.pilot.model.TestMetrics;
import com.personal.kafka.pilot.util.MavenDependencyResolver;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class KafkaLoadTestEngine<K, V> {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaLoadTestEngine.class);
    
    private final TestConfiguration config;
    private final MessageGenerator messageGenerator;
    private final TestMetrics metrics;
    private final Consumer<String> logConsumer;
    
    private KafkaProducer<K, V> producer;
    private ExecutorService executorService;
    private volatile boolean isRunning = false;
    private volatile boolean shouldStop = false;
    private ClassLoader customClassLoader = null;
    private final java.util.concurrent.ConcurrentHashMap<Integer, Long> partitionMaxOffset = new java.util.concurrent.ConcurrentHashMap<>();
    
    public KafkaLoadTestEngine(TestConfiguration config, 
                              MessageGenerator messageGenerator,
                              TestMetrics metrics,
                              Consumer<String> logConsumer) {
        this(config, messageGenerator, metrics, logConsumer, null);
    }
    
    public KafkaLoadTestEngine(TestConfiguration config, 
                              MessageGenerator messageGenerator,
                              TestMetrics metrics,
                              Consumer<String> logConsumer,
                              ClassLoader customClassLoader) {
        this.config = config;
        this.messageGenerator = messageGenerator;
        this.metrics = metrics;
        this.logConsumer = logConsumer;
        this.customClassLoader = customClassLoader;
        logger.info("KafkaLoadTestEngine initialized — POD: {}, Topic: {}, Messages: {}",
                   config.getPodNumber(), config.getTopicName(), config.getTotalMessageCount());
    }
    
    public void start() {
        if (isRunning) {
            log("Test is already running!");
            logger.warn("Attempted to start test while already running");
            return;
        }
        
        shouldStop = false;
        isRunning = true;
        
        log("=== Starting Kafka Load Test ===");
        
        try {
            loadCustomJarIfPresent();
            loadMavenDependencyIfPresent();
            initializeProducer();
            
            log("Bootstrap Servers: " + config.getBootstrapServers());
            log("Topic: " + config.getTopicName());
            log("Total Messages: " + config.getTotalMessageCount());
            log("Thread Pool Size: " + config.getThreadPoolSize());
            log("Batch Size: " + config.getBatchSize());
            log("Key Serializer: " + config.getKeySerializerClass());
            log("Value Serializer: " + config.getValueSerializerClass());
            log("");
            
            metrics.reset();
            metrics.setTotalMessages(config.getTotalMessageCount());
            metrics.setStartTime(System.currentTimeMillis());
            
            log("Generating " + config.getTotalMessageCount() + " messages...");
            List<V> messages = new ArrayList<>();
            List<K> keys = new ArrayList<>();
            List<Map<String, String>> headersPerMessage = new ArrayList<>();
            generateMessagesAndHeaders(messages, keys, headersPerMessage);
            log("Generated " + messages.size() + " messages, starting to publish...");
            publishRecords(messages, keys, headersPerMessage);
            
        } catch (Exception e) {
            log("ERROR: " + e.getMessage());
            logger.error("Load test error", e);
        } finally {
            cleanup();
        }
    }
    
    public void stop() {
        if (!isRunning) {
            log("No test is currently running");
            return;
        }
        shouldStop = true;
        log("Stopping test...");
    }
    
    private void initializeProducer() {
        try {
            Properties producerProperties = new Properties();
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
            producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
            producerProperties.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
            producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
            
            if (config.getAdditionalKafkaProperties() != null) {
                producerProperties.putAll(config.getAdditionalKafkaProperties());
            }
            
            // Resolve serializer class names
            String keySerializerClassName = config.getKeySerializerClass() != null
                    ? config.getKeySerializerClass() : StringSerializer.class.getName();
            String valueSerializerClassName = config.getValueSerializerClass() != null
                    ? config.getValueSerializerClass() : StringSerializer.class.getName();

            // Always instantiate serializers explicitly when flatbufMode is on, or when a
            // custom classloader is present.  Using class-name-in-properties causes Kafka's
            // internal type checker to reject byte[] values when the producer generic type
            // is inferred as String.
            if (config.isFlatbufMode() || customClassLoader != null) {
                ClassLoader loader = customClassLoader != null
                        ? customClassLoader : Thread.currentThread().getContextClassLoader();
                if (customClassLoader != null) Thread.currentThread().setContextClassLoader(customClassLoader);

                Class<?> keySerializerClass = Class.forName(keySerializerClassName, true, loader);
                org.apache.kafka.common.serialization.Serializer<K> keySerializer =
                        (org.apache.kafka.common.serialization.Serializer<K>) keySerializerClass.getDeclaredConstructor().newInstance();
                java.util.Map<String, Object> configMap = new java.util.HashMap<>();
                producerProperties.forEach((k, v) -> configMap.put((String) k, v));
                keySerializer.configure(configMap, true);

                Class<?> valueSerializerClass = Class.forName(valueSerializerClassName, true, loader);
                org.apache.kafka.common.serialization.Serializer<V> valueSerializer =
                        (org.apache.kafka.common.serialization.Serializer<V>) valueSerializerClass.getDeclaredConstructor().newInstance();
                valueSerializer.configure(configMap, false);

                log("Key Serializer: " + keySerializerClass.getName());
                log("Value Serializer: " + valueSerializerClass.getName());
                producer = new KafkaProducer<>(producerProperties, keySerializer, valueSerializer);
            } else {
                producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
                producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);
                log("Key Serializer: " + keySerializerClassName);
                log("Value Serializer: " + valueSerializerClassName);
                producer = new KafkaProducer<>(producerProperties);
            }
            log("Kafka producer initialized successfully");
            
        } catch (Exception e) {
            log("ERROR initializing producer: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            if (e.getCause() != null) log("Caused by: " + e.getCause().getMessage());
            logger.error("Failed to initialize Kafka producer", e);
            throw new RuntimeException("Failed to initialize Kafka producer", e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private void generateMessagesAndHeaders(List<V> messages, List<K> keys, List<Map<String, String>> headersPerMessage) {
        Map<String, String> configuredHeaders = config.getMessageHeaders();
        for (int i = 0; i < config.getTotalMessageCount(); i++) {
            if (shouldStop) break;
            Object[] result = messageGenerator.generateMessageAndJson(i);
            V message = (V) result[0];
            String messageJson = (String) result[1];
            messages.add(message);
            keys.add(resolveKeyFromJson(messageJson));

            if (i == 0 && config.isFlatbufMode()) {
                if (message instanceof byte[]) {
                    byte[] b = (byte[]) message;
                    StringBuilder head = new StringBuilder();
                    for (int j = 0; j < Math.min(b.length, 16); j++) head.append(String.format("%02x ", b[j]));
                    log("First message value type: byte[] (" + b.length + " bytes), head: " + head.toString().trim());
                } else {
                    log("First message value type: " + (message == null ? "null" : message.getClass().getName())
                            + " — NOTE: not byte[]; FlatBuffer/byte serialization is NOT active.");
                }
            }

            Map<String, String> resolved = new LinkedHashMap<>();
            if (configuredHeaders != null) {
                for (Map.Entry<String, String> entry : configuredHeaders.entrySet()) {
                    String resolvedValue = resolveHeaderValue(entry.getKey(), entry.getValue(), messageJson);
                    if (!resolvedValue.isEmpty()) {
                        resolved.put(entry.getKey(), resolvedValue);
                    }
                }
            }
            headersPerMessage.add(resolved);

            if ((i + 1) % 10000 == 0) {
                log("Generated " + (i + 1) + " messages...");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private K resolveKeyFromJson(String messageJson) {
        String keyType = config.getMessageKeyType();
        if (keyType == null || keyType.equals("none")) return null;

        String keyStr = null;
        if (keyType.equals("hardcoded")) {
            keyStr = config.getMessageKeyHardcoded();
        } else if (keyType.equals("field")) {
            String fieldPath = config.getMessageKeyField();
            if (fieldPath == null || fieldPath.trim().isEmpty()) return null;
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(messageJson);
                for (String part : fieldPath.trim().split("\\.")) {
                    if (node == null || !node.has(part)) { node = null; break; }
                    node = node.get(part);
                }
                if (node != null && !node.isNull() && node.isValueNode()) keyStr = node.asText();
            } catch (Exception e) {
                logger.warn("Could not resolve key field '{}' from message JSON: {}", fieldPath, e.getMessage());
            }
        } else {
            String keyConfig = config.getMessageKeyField();
            if (keyConfig == null || keyConfig.trim().isEmpty()) return null;
            keyStr = keyConfig.trim();
        }

        if (keyStr == null) return null;
        // When flatbuf mode is active the producer uses ByteArraySerializer — key must be byte[]
        if (config.isFlatbufMode()) {
            return (K) keyStr.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        return (K) keyStr;
    }

    
    private void publishRecords(List<V> messages, List<K> keys, List<Map<String, String>> headersPerMessage) {
        List<ProducerRecord<K, V>> records = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            K key = keys.get(i);
            ProducerRecord<K, V> record = config.getTargetPartition() != null
                ? new ProducerRecord<>(config.getTopicName(), config.getTargetPartition(), key, messages.get(i))
                : new ProducerRecord<>(config.getTopicName(), key, messages.get(i));
            headersPerMessage.get(i).forEach((hk, hv) ->
                record.headers().add(hk, hv.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            records.add(record);
        }

        int threadCount = Math.min(config.getThreadPoolSize(), (records.size() / config.getBatchSize()) + 1);
        executorService = Executors.newFixedThreadPool(threadCount);
        List<List<ProducerRecord<K, V>>> partitions = Lists.partition(records, config.getBatchSize());
        CountDownLatch latch = new CountDownLatch(partitions.size());
        log("Publishing using " + threadCount + " threads, " + partitions.size() + " batches...");

        int batchNumber = 0;
        for (List<ProducerRecord<K, V>> partition : partitions) {
            final int currentBatch = ++batchNumber;
            executorService.execute(() -> {
                try {
                    for (int i = 0; i < partition.size(); i++) {
                        if (shouldStop) break;
                        ProducerRecord<K, V> record = partition.get(i);
                        String recordKey = record.key() != null ? record.key().toString() : "null";
                        long sendStartTime = System.currentTimeMillis();
                        producer.send(record, (metadata, exception) -> {
                            long latency = System.currentTimeMillis() - sendStartTime;
                            if (exception == null) {
                                metrics.recordSuccess(latency);
                                partitionMaxOffset.merge(metadata.partition(), metadata.offset(), Math::max);
                                int totalSent = metrics.getTotalSent();
                                log(String.format("[%d/%d] key=%s partition=%d offset=%d latency=%dms",
                                    totalSent, config.getTotalMessageCount(),
                                    recordKey, metadata.partition(), metadata.offset(), latency));
                                if (totalSent % 1000 == 0) {
                                    log(String.format("--- Progress: %d/%d (%.1f%%) | Success: %d | Failed: %d | Rate: %.1f msg/s",
                                        totalSent, config.getTotalMessageCount(),
                                        metrics.getProgressPercentage(),
                                        metrics.getSuccessCount().get(),
                                        metrics.getFailureCount().get(),
                                        metrics.getMessagesPerSecond()));
                                }
                            } else {
                                metrics.recordFailure();
                                StringBuilder err = new StringBuilder("ERROR: ");
                                for (Throwable t = exception; t != null; t = t.getCause()) {
                                    err.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
                                    if (t.getCause() != null) err.append(" | ");
                                }
                                log(err.toString());
                                logger.error("Send failed", exception);
                            }
                        });
                    }
                } catch (Exception e) {
                    log("ERROR in batch " + currentBatch + ": " + e.getMessage());
                    logger.error("Error in batch {}", currentBatch, e);
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await();
            producer.flush();
            metrics.setEndTime(System.currentTimeMillis());
            printSummary();
        } catch (InterruptedException e) {
            log("Test interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }
    
    private void printSummary() {
        log("");
        log("=== Test Summary ===");
        log("Total Sent:      " + metrics.getTotalSent());
        log("Successful:      " + metrics.getSuccessCount().get());
        log("Failed:          " + metrics.getFailureCount().get());
        log("Success Rate:    " + String.format("%.2f%%", metrics.getSuccessRate()));
        log("Avg Latency:     " + String.format("%.2f ms", metrics.getAverageLatency()));
        log("Total Time:      " + metrics.getElapsedTime() + " ms");
        log("Throughput:      " + String.format("%.1f msg/s", metrics.getMessagesPerSecond()));
        log("===================");
    }
    
    private void cleanup() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) executorService.shutdownNow();
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (producer != null) producer.close();
        isRunning = false;
    }
    
    private void loadMavenDependencyIfPresent() {
        if (config.getMavenDependency() == null || config.getMavenDependency().trim().isEmpty()) return;
        if (customClassLoader != null) {
            Thread.currentThread().setContextClassLoader(customClassLoader);
            log("Using existing custom classloader");
            return;
        }
        try {
            String dependency = config.getMavenDependency().trim();
            log("Loading Maven dependency: " + dependency);
            MavenDependencyResolver resolver = new MavenDependencyResolver();
            MavenDependencyResolver.MavenDependency dep = MavenDependencyResolver.MavenDependency.parse(dependency);
            URLClassLoader classLoader = resolver.loadDependencyIntoClassLoader(
                dep.getGroupId(), dep.getArtifactId(), dep.getVersion());
            this.customClassLoader = classLoader;
            Thread.currentThread().setContextClassLoader(classLoader);
            log("Maven dependency loaded: " + dependency);
        } catch (Exception e) {
            log("ERROR loading Maven dependency: " + e.getMessage());
            throw new RuntimeException("Failed to load Maven dependency: " + config.getMavenDependency(), e);
        }
    }
    
    private void loadCustomJarIfPresent() {
        if (config.getCustomJarPath() == null || config.getCustomJarPath().trim().isEmpty()) return;
        try {
            File jarFile = new File(config.getCustomJarPath());
            if (!jarFile.exists()) { log("WARNING: Custom JAR not found: " + config.getCustomJarPath()); return; }
            if (!jarFile.getName().endsWith(".jar")) { log("WARNING: Not a .jar file: " + config.getCustomJarPath()); return; }
            log("Loading custom JAR: " + jarFile.getName());
            URLClassLoader classLoader = new URLClassLoader(
                new URL[]{jarFile.toURI().toURL()}, Thread.currentThread().getContextClassLoader());
            this.customClassLoader = classLoader;
            Thread.currentThread().setContextClassLoader(classLoader);
            log("Custom JAR loaded: " + jarFile.getName());
        } catch (Exception e) {
            log("ERROR loading custom JAR: " + e.getMessage());
            throw new RuntimeException("Failed to load custom JAR: " + config.getCustomJarPath(), e);
        }
    }
    
    private String resolveHeaderValue(String headerKey, String configuredValue, String messageJson) {
        String fieldPath = (configuredValue == null || configuredValue.isEmpty()) ? headerKey : configuredValue;
        if (messageJson != null) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                com.fasterxml.jackson.databind.JsonNode node = mapper.readTree(messageJson);
                for (String part : fieldPath.split("\\.")) {
                    if (node == null || !node.has(part)) { node = null; break; }
                    node = node.get(part);
                }
                if (node != null && !node.isNull() && node.isValueNode()) return node.asText();
            } catch (Exception e) {
                logger.debug("Could not resolve header '{}': {}", fieldPath, e.getMessage());
            }
        }
        return (configuredValue == null || configuredValue.isEmpty()) ? "" : configuredValue;
    }

    private void log(String message) {
        if (logConsumer != null) logConsumer.accept(message);
        logger.info(message);
    }
    
    public boolean isRunning() {
        return isRunning;
    }

    public Map<Integer, Long> getPartitionMaxOffsets() {
        return Collections.unmodifiableMap(partitionMaxOffset);
    }
}
