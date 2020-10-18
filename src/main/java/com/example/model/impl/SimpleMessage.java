package com.example.model.impl;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

import com.example.model.Message;

public class SimpleMessage implements Comparable<SimpleMessage>, Message {

    private final UUID id;

    private final String payload;

    private final Instant creationTime;

    private SimpleMessage(final UUID id, final String payload) {
        this.id = id;
        this.payload = payload;
        this.creationTime = Instant.now();
    }

    private SimpleMessage(final UUID id, final String payload, final Instant creationTime) {
        this.id = id;
        this.payload = payload;
        this.creationTime = creationTime;
    }

    public UUID getId() {
        return id;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    public Instant getCreationTime() {
        return creationTime;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", payload='" + payload + '\'' +
                ", creationTime=" + creationTime +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleMessage that = (SimpleMessage) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payload, creationTime);
    }

    @Override
    public int compareTo(SimpleMessage message) {
        return creationTime.compareTo(message.getCreationTime());
    }

    public static StringMessageBuilder builder() {
        return new StringMessageBuilder();
    }

    public StringMessageBuilder toBuilder() {
        return new StringMessageBuilder()
                .id(id)
                .payload(payload);
    }

    public static class StringMessageBuilder {

        private UUID id;

        private String payload;

        private Instant creationTime;

        public StringMessageBuilder id(final UUID id) {
            this.id = id;
            return this;
        }

        public StringMessageBuilder payload(final String payload) {
            this.payload = payload;
            return this;
        }

        public StringMessageBuilder creationTimeNano(final Instant creationTime) {
            this.creationTime = creationTime;
            return this;
        }

        public SimpleMessage build() {
            if (Objects.isNull(creationTime)) {
                return new SimpleMessage(id, payload);
            }
            return new SimpleMessage(id, payload, creationTime);
        }
    }
}