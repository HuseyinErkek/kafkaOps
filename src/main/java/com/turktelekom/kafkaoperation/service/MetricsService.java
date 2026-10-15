package com.turktelekom.kafkaoperation.service;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class MetricsService {
    private static final int MAX_EVENTS = 50;

    private final SimpMessagingTemplate messagingTemplate;
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private final AtomicLong ackedMessages = new AtomicLong(0);
    private final AtomicLong failedMessages = new AtomicLong(0);
    private final AtomicLong pendingMessages = new AtomicLong(0);

    private volatile Timer latencyTimer = createTimer();
    private final Queue<String> eventLog = new ConcurrentLinkedQueue<>();

    private volatile long lastAckedCount = 0;

    private Timer createTimer() {
        return Timer.builder("kafka.producer.latency")
                .publishPercentiles(0.95, 0.99)
                .register(registry);
    }

    public void recordSuccess(long latencyMs) {
        ackedMessages.incrementAndGet();
        latencyTimer.record(latencyMs, TimeUnit.MILLISECONDS);
    }

    public void recordFailure() { failedMessages.incrementAndGet(); }
    public void incrementPending() { pendingMessages.incrementAndGet(); }
    public void decrementPending() { pendingMessages.decrementAndGet(); }

    public void logEvent(String event) {
        String timestamp = java.time.LocalTime.now().withNano(0).toString();
        eventLog.add(timestamp + " - " + event);
        while (eventLog.size() > MAX_EVENTS) {
            eventLog.poll();
        }
    }

    public synchronized void reset() {
        ackedMessages.set(0);
        failedMessages.set(0);
        pendingMessages.set(0);
        lastAckedCount = 0; // sıfırlanmazsa ilk saniyedeki hız negatif çıkar
        registry.remove(latencyTimer);
        latencyTimer = createTimer();
        eventLog.clear();
    }

    @Scheduled(fixedRate = 1000)
    public synchronized void broadcastMetrics() {
        long currentAcked = ackedMessages.get();
        long rate = currentAcked - lastAckedCount;
        lastAckedCount = currentAcked;

        Timer timer = latencyTimer;
        double p95 = 0;
        HistogramSnapshot snapshot = timer.takeSnapshot();
        for (ValueAtPercentile v : snapshot.percentileValues()) {
            if (Math.abs(v.percentile() - 0.95) < 1e-9) {
                p95 = v.value(TimeUnit.MILLISECONDS);
            }
        }

        MetricDto dto = new MetricDto(
                currentAcked,
                failedMessages.get(),
                Math.max(0, pendingMessages.get()),
                Math.max(0, rate),
                timer.mean(TimeUnit.MILLISECONDS),
                p95,
                new ArrayList<>(eventLog) // JSON'a çevrilirken değişmesin diye kopya
        );
        messagingTemplate.convertAndSend("/topic/metrics", dto);
    }
}
