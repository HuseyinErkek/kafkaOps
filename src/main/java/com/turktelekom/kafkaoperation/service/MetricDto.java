package com.turktelekom.kafkaoperation.service;

import java.util.Collection;

public record MetricDto(
        long ackedMessages,
        long failedMessages,
        long pendingMessages,
        long rate,
        double avgLatency,
        double p95Latency,
        Collection<String> events
) {}