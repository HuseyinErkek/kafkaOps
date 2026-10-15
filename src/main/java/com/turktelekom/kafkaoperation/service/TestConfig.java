package com.turktelekom.kafkaoperation.service;

import lombok.Data;

/**
 * Test parametreleri. Alanlar eksik gelirse varsayılan değerler kullanılır.
 * mode: CONTINUOUS (hedef hızda sürekli) veya BURST (hız sınırı olmadan, totalMessages kadar)
 */
@Data
public class TestConfig {
    private String topic = "broker-load-test";
    private int messageSize = 1000;
    private long totalMessages = 100_000;
    private int targetRate = 1000;
    private int concurrency = 1;
    private String mode = "CONTINUOUS";
}
