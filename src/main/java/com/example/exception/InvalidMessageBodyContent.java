package com.example.exception;

public class InvalidMessageBodyContent extends RuntimeException {

    public InvalidMessageBodyContent() {
    }

    public InvalidMessageBodyContent(final String message) {
        super(message);
    }

    public InvalidMessageBodyContent(final String message, final Throwable cause) {
        super(message, cause);
    }
}
