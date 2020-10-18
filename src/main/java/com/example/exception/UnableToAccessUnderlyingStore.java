package com.example.exception;

public class UnableToAccessUnderlyingStore extends RuntimeException {

    public UnableToAccessUnderlyingStore() {
    }

    public UnableToAccessUnderlyingStore(String message) {
        super(message);
    }

    public UnableToAccessUnderlyingStore(String message, Throwable cause) {
        super(message, cause);
    }
}
