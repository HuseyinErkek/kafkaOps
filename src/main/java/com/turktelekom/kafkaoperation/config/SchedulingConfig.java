package com.turktelekom.kafkaoperation.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * MetricsService.broadcastMetrics() içindeki @Scheduled'ın çalışması için gerekli.
 * Ana sınıfta zaten @EnableScheduling varsa bu dosyayı eklemene gerek yok (eklenmesi de zararsız).
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}
