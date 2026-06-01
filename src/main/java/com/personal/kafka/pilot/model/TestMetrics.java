package com.personal.kafka.pilot.model;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class TestMetrics {
    
    private final AtomicInteger successCount = new AtomicInteger(0);
    
    private final AtomicInteger failureCount = new AtomicInteger(0);
    
    private final AtomicLong totalLatency = new AtomicLong(0);
    
    private long startTime;
    
    private long endTime;
    
    private int totalMessages;
    
    public void recordSuccess(long latency) {
        successCount.incrementAndGet();
        totalLatency.addAndGet(latency);
    }
    
    public void recordFailure() {
        failureCount.incrementAndGet();
    }
    
    public int getTotalSent() {
        return successCount.get() + failureCount.get();
    }
    
    public double getSuccessRate() {
        int total = getTotalSent();
        if (total == 0) return 0.0;
        return (successCount.get() * 100.0) / total;
    }
    
    public double getAverageLatency() {
        int count = successCount.get();
        if (count == 0) return 0.0;
        return totalLatency.get() / (double) count;
    }
    
    public long getElapsedTime() {
        if (startTime == 0) return 0;
        long end = endTime > 0 ? endTime : System.currentTimeMillis();
        return end - startTime;
    }
    
    public double getMessagesPerSecond() {
        long elapsed = getElapsedTime();
        if (elapsed == 0) return 0.0;
        return (getTotalSent() * 1000.0) / elapsed;
    }
    
    public double getProgressPercentage() {
        if (totalMessages == 0) return 0.0;
        return (getTotalSent() * 100.0) / totalMessages;
    }
    
    public void reset() {
        successCount.set(0);
        failureCount.set(0);
        totalLatency.set(0);
        startTime = 0;
        endTime = 0;
        totalMessages = 0;
    }
}
