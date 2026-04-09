package org.example.notificationservice.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.notificationservice.event.VerificationRequestedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
class NotificationServiceIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
    );

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest"); // важно для тестов
    }

    @Test
    @DisplayName("Should consume verification event from Kafka topic")
    void shouldConsumeVerificationEvent(CapturedOutput output) {
        VerificationRequestedEvent event = new VerificationRequestedEvent(
                "user-id-1",
                "integration@example.com",
                "123456",
                Instant.now().plus(15, ChronoUnit.MINUTES)
        );

        sendEvent(event);

        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    String logs = output.getOut();

                    assertThat(logs)
                            .contains("Received verification event")
                            .contains("userId=user-id-1")
                            .contains("email=integration@example.com")
                            .contains("Code for integration@example.com: 123456")
                            .contains("Code expires at:");
                });
    }

    @Test
    @DisplayName("Should consume multiple events from Kafka topic")
    void shouldConsumeMultipleEvents(CapturedOutput output) {
        for (int i = 0; i < 5; i++) {
            VerificationRequestedEvent event = new VerificationRequestedEvent(
                    "user-id-" + i,
                    "user" + i + "@example.com",
                    String.format("%06d", i),
                    Instant.now().plus(15, ChronoUnit.MINUTES)
            );
            sendEvent(event);
        }

        await()
                .atMost(10, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    String logs = output.getOut();
                    assertThat(logs).contains("Received verification event").contains("user-id-0")
                            .contains("user-id-4")
                            .contains("Code for user4@example.com:");
                });
    }

    private void sendEvent(VerificationRequestedEvent event) {
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class
        );

        try (KafkaProducer<String, VerificationRequestedEvent> producer = new KafkaProducer<>(producerProps)) {
            producer.send(new ProducerRecord<>("verification-codes", event.email(), event));
            producer.flush();
        }
    }

}
