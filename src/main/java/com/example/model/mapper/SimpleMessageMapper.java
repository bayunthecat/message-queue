package com.example.model.mapper;

import java.time.Instant;
import java.util.UUID;

import com.example.exception.RowMappingException;
import com.example.model.impl.SimpleMessage;

public class SimpleMessageMapper {

    private static final String DELIMITER = ",";

    public String toString(final SimpleMessage message) {
        return message.getId() + DELIMITER + message.getPayload() + DELIMITER + message
                .getCreationTime().toEpochMilli() + System.lineSeparator();
    }

    public SimpleMessage toMessage(final String messageAsString) {
        try {
            final String[] parts = messageAsString.split(DELIMITER);
            return SimpleMessage.builder()
                                .id(UUID.fromString(parts[0]))
                                .payload(parts[1])
                                .creationTimeNano(Instant.ofEpochMilli(Long.valueOf(parts[2])))
                                .build();
        } catch (Exception e) {
            throw new RowMappingException(e.getMessage(), e);
        }
    }
}
