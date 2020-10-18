package com.example.model.impl;

import com.example.model.Message;

public class AmazonSqsMessage implements Message {

    private final String payload;

    private final String receiptHandle;

    private AmazonSqsMessage(final String payload, final String receiptHandle) {
        this.payload = payload;
        this.receiptHandle = receiptHandle;
    }

    @Override
    public String getPayload() {
        return payload;
    }

    public String getReceiptHandle() {
        return receiptHandle;
    }

    public static AmazonSqsMessageBuilder builder() {
        return new AmazonSqsMessageBuilder();
    }

    public static class AmazonSqsMessageBuilder {

        private String payload;

        private String receiptHandle;

        public AmazonSqsMessageBuilder payload(String payload) {
            this.payload = payload;
            return this;
        }

        public AmazonSqsMessageBuilder receiptHandle(String receiptHandle) {
            this.receiptHandle = receiptHandle;
            return this;
        }

        public AmazonSqsMessage build() {
            return new AmazonSqsMessage(payload, receiptHandle);
        }
    }
}