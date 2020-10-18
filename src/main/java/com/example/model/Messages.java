package com.example.model;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.example.model.impl.SimpleMessage;

public class Messages {

    public static SimpleMessage createMessage(final String payload) {
        return SimpleMessage.builder().id(UUID.randomUUID()).payload(payload).build();
    }

    public static SimpleMessage createMessage(final SimpleMessage message) {
        return message.toBuilder().build();
    }

    public static boolean isExpired(final SimpleMessage message, final long visibilityTimeout) {
        return Objects.nonNull(message) && Instant.now()
                                                  .isAfter(message.getCreationTime().plusMillis(visibilityTimeout));
    }
}
