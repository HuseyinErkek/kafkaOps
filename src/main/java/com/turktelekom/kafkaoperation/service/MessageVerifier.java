package com.turktelekom.kafkaoperation.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Tüketici tarafı doğrulama.
 *
 * Eski sürüm "sıradaki seq" beklediği için birden fazla partition veya thread olduğunda
 * (sıra garantisi yok) yanlış "mesaj kaybı" uyarısı veriyordu. Bu sürüm sıradan bağımsız çalışır:
 * gelen her seq'i işaretler, test bittikten sonra eksik ve tekrar eden mesajları raporlar.
 *
 * Mesaj formatı: messageId|seq|runId|padding
 * Başka bir testten (runId farklı) kalan mesajlar yok sayılır.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageVerifier {

    private final MetricsService metricsService;

    @Value("${test.topic:broker-load-test}")
    private String listenedTopic;

    private final Object lock = new Object();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "verifier-scheduler");
        t.setDaemon(true);
        return t;
    });

    private BitSet received = new BitSet();
    private long currentRunId = -1;
    private long uniqueCount = 0;
    private long duplicateCount = 0;

    public String getListenedTopic() {
        return listenedTopic;
    }

    public void reset(long runId) {
        synchronized (lock) {
            received = new BitSet();
            currentRunId = runId;
            uniqueCount = 0;
            duplicateCount = 0;
        }
    }

    @KafkaListener(topics = "${test.topic:broker-load-test}", groupId = "verification-group")
    public void listen(ConsumerRecord<String, String> record) {
        String v = record.value();
        if (v == null) return;

        try {
            int p1 = v.indexOf('|');
            int p2 = p1 < 0 ? -1 : v.indexOf('|', p1 + 1);
            int p3 = p2 < 0 ? -1 : v.indexOf('|', p2 + 1);
            if (p3 < 0) {
                log.debug("Beklenen formatta olmayan mesaj atlandı (offset {})", record.offset());
                return;
            }

            long seq = Long.parseLong(v, p1 + 1, p2, 10);
            long msgRunId = Long.parseLong(v, p2 + 1, p3, 10);
            if (seq < 1 || seq > Integer.MAX_VALUE) return;

            synchronized (lock) {
                if (msgRunId != currentRunId) return; // eski testten kalan mesaj

                int idx = (int) seq;
                if (received.get(idx)) {
                    duplicateCount++;
                    if (duplicateCount <= 10) {
                        metricsService.logEvent("Yinelenen mesaj (Duplicate) tespit edildi: Seq " + seq);
                    }
                } else {
                    received.set(idx);
                    uniqueCount++;
                }
            }
        } catch (NumberFormatException e) {
            log.debug("Mesaj formatı geçersiz (offset {})", record.offset());
        }
    }

    /**
     * Test bittikten sonra tüketicinin yetişmesini bekler, ilerleme durunca veya
     * hepsi gelince (en fazla ~60 sn) sonucu olay günlüğüne yazar.
     */
    public void verifyAfterTest(long runId, long sentCount) {
        scheduleCheck(runId, sentCount, -1, 0);
    }

    private void scheduleCheck(long runId, long sentCount, long lastUnique, int attempt) {
        scheduler.schedule(() -> {
            long unique;
            synchronized (lock) {
                if (currentRunId != runId) return; // arada yeni test başladı
                unique = uniqueCount;
            }
            boolean complete = unique >= sentCount;
            boolean noProgress = unique == lastUnique;
            if (complete || noProgress || attempt >= 20) {
                report(runId, sentCount);
            } else {
                scheduleCheck(runId, sentCount, unique, attempt + 1);
            }
        }, 3, TimeUnit.SECONDS);
    }

    private void report(long runId, long sentCount) {
        long unique;
        long duplicates;
        List<Long> missing = new ArrayList<>();

        synchronized (lock) {
            if (currentRunId != runId) return;
            unique = uniqueCount;
            duplicates = duplicateCount;
            int idx = received.nextClearBit(1);
            while (idx <= sentCount && missing.size() < 5) {
                missing.add((long) idx);
                idx = received.nextClearBit(idx + 1);
            }
        }

        long lost = Math.max(0, sentCount - unique);
        if (lost == 0 && duplicates == 0) {
            metricsService.logEvent("✅ Doğrulama tamam: gönderilen " + sentCount
                    + ", tüketilen " + unique + ", kayıp yok, tekrar yok.");
            return;
        }
        if (lost > 0) {
            metricsService.logEvent("MESAJ KAYBI TESPİT EDİLDİ! Gönderilen: " + sentCount
                    + ", tüketilen: " + unique + ", eksik: " + lost
                    + " (ilk eksik seq: " + missing + "; producer hataları dahil)");
        }
        if (duplicates > 0) {
            metricsService.logEvent("Toplam " + duplicates + " yinelenen mesaj tespit edildi.");
        }
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdownNow();
    }
}
