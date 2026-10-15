package com.turktelekom.kafkaoperation.controller;

import com.turktelekom.kafkaoperation.service.KafkaTestService;
import com.turktelekom.kafkaoperation.service.MetricsService;
import com.turktelekom.kafkaoperation.service.TestConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@CrossOrigin(origins = "*")

@RestController
@RequestMapping("/api/test")
@RequiredArgsConstructor
public class TestController {
    private final KafkaTestService testService;
    private final MetricsService metricsService;

    @PostMapping("/start")
    public ResponseEntity<Map<String, String>> start(@RequestBody TestConfig config) {
        boolean started = testService.startTest(config);
        if (!started) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(Map.of("error", "Zaten çalışan bir test var (veya önceki test henüz sonlanıyor)."));
        }
        return ResponseEntity.ok(Map.of("status", "started"));
    }

    @PostMapping("/stop")
    public ResponseEntity<Map<String, String>> stop() {
        boolean stopped = testService.stopTest();
        return ResponseEntity.ok(Map.of("status", stopped ? "stopped" : "not_running"));
    }

    @PostMapping("/mark")
    public ResponseEntity<Map<String, String>> markEvent(@RequestParam(defaultValue = "işaret") String note) {
        metricsService.logEvent("📍 İşaret: " + note);
        return ResponseEntity.ok(Map.of("status", "marked"));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}
