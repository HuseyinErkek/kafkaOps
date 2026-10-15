package com.turktelekom.kafkaoperation.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaTestService {

    private static final String MODE_CONTINUOUS = "CONTINUOUS";
    private static final String MODE_BURST = "BURST";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MetricsService metricsService;
    private final MessageVerifier messageVerifier;

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicLong sequenceGenerator = new AtomicLong(0);
    private final AtomicLong sentCounter = new AtomicLong(0);
    private final AtomicLong runIdGenerator = new AtomicLong(0);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);
    private final AtomicLong lastFailureLogMs = new AtomicLong(0);

    private volatile ExecutorService loadExecutor;

    /**
     * @return false: zaten çalışan bir test var, true: test başlatıldı
     * @throws IllegalArgumentException parametreler geçersizse
     */
    public synchronized boolean startTest(TestConfig config) {
        validate(config);

        if (isRunning.get() || activeWorkers.get() > 0) {
            return false;
        }
        isRunning.set(true);

        long runId = runIdGenerator.incrementAndGet();
        sequenceGenerator.set(0);
        sentCounter.set(0);
        metricsService.reset();
        messageVerifier.reset(runId);

        int workers = config.getConcurrency();
        boolean burst = MODE_BURST.equals(config.getMode());
        boolean continuous = MODE_CONTINUOUS.equals(config.getMode());

        // Her worker toplam hızın 1/workers kadarını üstlenir. BURST modunda bekleme yok.
        long delayNs = burst ? 0 : (1_000_000_000L * workers) / config.getTargetRate();
        String padding = "x".repeat(config.getMessageSize());

        metricsService.logEvent(String.format(
                "Test başlatıldı. Mod: %s, Topic: %s, Boyut: %d, Hız: %s, Thread: %d",
                config.getMode(), config.getTopic(), config.getMessageSize(),
                burst ? "sınırsız" : config.getTargetRate() + " msg/sn", workers));

        if (!config.getTopic().equals(messageVerifier.getListenedTopic())) {
            metricsService.logEvent("UYARI: Doğrulama sadece '" + messageVerifier.getListenedTopic()
                    + "' topic'ini dinler, '" + config.getTopic() + "' için kayıp tespiti yapılmaz.");
        }

        activeWorkers.set(workers);
        loadExecutor = Executors.newFixedThreadPool(workers);
        for (int i = 0; i < workers; i++) {
            loadExecutor.submit(() -> runWorker(config, runId, padding, delayNs, continuous));
        }
        return true;
    }

    private void runWorker(TestConfig config, long runId, String padding, long delayNs, boolean continuous) {
        try {
            long total = config.getTotalMessages();
            long nextSendTime = System.nanoTime();

            while (isRunning.get()) {
                if (delayNs > 0) {
                    long wait = nextSendTime - System.nanoTime();
                    if (wait > 0) {
                        LockSupport.parkNanos(Math.min(wait, 1_000_000L));
                        continue;
                    }
                }

                long seq = sequenceGenerator.incrementAndGet();
                if (!continuous && seq > total) {
                    break;
                }

                sendMessageAsync(config.getTopic(), runId, seq, config.getMessageSize(), padding);

                if (delayNs > 0) {
                    nextSendTime += delayNs;
                    // Çok geride kalındıysa telafi patlaması yapma, saati sıfırla
                    if (System.nanoTime() - nextSendTime > 1_000_000_000L) {
                        nextSendTime = System.nanoTime();
                    }
                }
            }
        } catch (Throwable t) {
            log.error("Worker beklenmeyen hata ile durdu", t);
            metricsService.logEvent("Worker hatası: " + t.getMessage());
        } finally {
            if (activeWorkers.decrementAndGet() == 0) {
                finishRun(runId);
            }
        }
    }

    private void sendMessageAsync(String topic, long runId, long seq, int size, String padding) {
        String messageId = UUID.randomUUID().toString();

        // Format: messageId|seq|runId|padding  (toplam uzunluk ~ size)
        StringBuilder sb = new StringBuilder(size + 64);
        sb.append(messageId).append('|').append(seq).append('|').append(runId).append('|');
        int padLen = Math.min(Math.max(0, size - sb.length()), padding.length());
        sb.append(padding, 0, padLen);

        long startNs = System.nanoTime();
        metricsService.incrementPending();
        sentCounter.incrementAndGet();

        try {
            kafkaTemplate.send(topic, messageId, sb.toString()).whenComplete((result, ex) -> {
                long latencyMs = (System.nanoTime() - startNs) / 1_000_000;
                metricsService.decrementPending();

                if (ex == null) {
                    metricsService.recordSuccess(latencyMs);
                } else {
                    metricsService.recordFailure();
                    logFailureThrottled(seq, ex);
                }
            });
        } catch (Exception e) {
            // send() bazen (ör. max.block.ms aşımı) senkron olarak da hata fırlatır
            metricsService.decrementPending();
            metricsService.recordFailure();
            logFailureThrottled(seq, e);
        }
    }

    // Broker düştüğünde saniyede binlerce hata günlüğünü engellemek için saniyede en fazla 1 kayıt
    private void logFailureThrottled(long seq, Throwable ex) {
        long now = System.currentTimeMillis();
        long last = lastFailureLogMs.get();
        if (now - last >= 1000 && lastFailureLogMs.compareAndSet(last, now)) {
            metricsService.logEvent("Broker bağlantı hatası/Timeout (Seq: " + seq + "): " + ex.getMessage());
        }
    }

    /** @return true: çalışan test durduruldu, false: zaten çalışan test yoktu */
    public boolean stopTest() {
        if (!isRunning.compareAndSet(true, false)) {
            return false;
        }
        ExecutorService executor = loadExecutor;
        if (executor != null) {
            executor.shutdown();
        }
        metricsService.logEvent("Test durduruldu.");
        return true;
    }

    // Tüm worker'lar bittiğinde (süre/adet doldu veya kullanıcı durdurdu) bir kez çalışır
    private void finishRun(long runId) {
        stopTest();
        long sent = sentCounter.get();
        metricsService.logEvent("Gönderim bitti: " + sent + " mesaj gönderildi. Tüketici doğrulaması bekleniyor...");
        messageVerifier.verifyAfterTest(runId, sent);
    }

    private void validate(TestConfig c) {
        if (c.getTopic() == null || c.getTopic().isBlank()) {
            throw new IllegalArgumentException("Topic boş olamaz.");
        }
        if (c.getMode() == null) {
            throw new IllegalArgumentException("Mod gerekli (CONTINUOUS veya BURST).");
        }
        c.setMode(c.getMode().trim().toUpperCase(Locale.ROOT));
        if (!MODE_CONTINUOUS.equals(c.getMode()) && !MODE_BURST.equals(c.getMode())) {
            throw new IllegalArgumentException("Geçersiz mod: " + c.getMode() + " (CONTINUOUS veya BURST olmalı).");
        }
        if (c.getMessageSize() < 1 || c.getMessageSize() > 1_000_000) {
            throw new IllegalArgumentException("Mesaj boyutu 1 ile 1.000.000 arasında olmalı.");
        }
        if (c.getConcurrency() < 1 || c.getConcurrency() > 64) {
            throw new IllegalArgumentException("Paralel thread sayısı 1 ile 64 arasında olmalı.");
        }
        if (MODE_BURST.equals(c.getMode()) && c.getTotalMessages() < 1) {
            throw new IllegalArgumentException("Mesaj sayısı en az 1 olmalı.");
        }
        if (MODE_CONTINUOUS.equals(c.getMode()) && c.getTargetRate() < 1) {
            throw new IllegalArgumentException("Gönderim hızı en az 1 msg/sn olmalı.");
        }
    }

    @PreDestroy
    public void shutdown() {
        isRunning.set(false);
        ExecutorService executor = loadExecutor;
        if (executor != null) {
            executor.shutdownNow();
        }
    }
}
