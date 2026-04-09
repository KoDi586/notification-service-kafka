package org.example.notificationservice.consumer;

import org.example.notificationservice.event.VerificationRequestedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class VerificationCodeConsumer {

    private static final Logger log = LoggerFactory.getLogger(VerificationCodeConsumer.class);

    @KafkaListener(topics = "verification-codes", groupId = "notification-service-group")
    public void consume(VerificationRequestedEvent event) {
        log.info("Received verification event: userId={}, email={}", event.userId(), event.email());
        log.info("Code for {}: {}", event.email(), event.code());
        log.info("Code expires at: {}", event.expiresAt());
    }
}
