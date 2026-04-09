package org.example.notificationservice.consumer;

import org.example.notificationservice.event.VerificationRequestedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThatCode;

@ExtendWith(MockitoExtension.class)
class VerificationCodeConsumerTest {

    private final VerificationCodeConsumer consumer = new VerificationCodeConsumer();

    @Test
    @DisplayName("Should consume verification event without errors")
    void shouldConsumeEvent() {
        VerificationRequestedEvent event = new VerificationRequestedEvent(
                "user-id-1",
                "test@example.com",
                "123456",
                Instant.now().plus(15, ChronoUnit.MINUTES)
        );

        assertThatCode(() -> consumer.consume(event)).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle multiple events")
    void shouldHandleMultipleEvents() {
        for (int i = 0; i < 10; i++) {
            VerificationRequestedEvent event = new VerificationRequestedEvent(
                    "user-id-" + i,
                    "user" + i + "@example.com",
                    String.format("%06d", i),
                    Instant.now().plus(15, ChronoUnit.MINUTES)
            );
            assertThatCode(() -> consumer.consume(event)).doesNotThrowAnyException();
        }
    }

    @Test
    @DisplayName("Should handle event with expired code")
    void shouldHandleExpiredEvent() {
        VerificationRequestedEvent event = new VerificationRequestedEvent(
                "user-id-1",
                "expired@example.com",
                "999999",
                Instant.now().minus(5, ChronoUnit.MINUTES)
        );

        assertThatCode(() -> consumer.consume(event)).doesNotThrowAnyException();
    }
}
