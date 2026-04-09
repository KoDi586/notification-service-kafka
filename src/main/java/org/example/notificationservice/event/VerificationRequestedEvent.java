package org.example.notificationservice.event;

import java.time.Instant;

public record VerificationRequestedEvent(
        String userId,
        String email,
        String code,
        Instant expiresAt
) {
}
