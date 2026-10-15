package com.turktelekom.kafkaoperation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaoperationApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaoperationApplication.class, args);
	}

}
