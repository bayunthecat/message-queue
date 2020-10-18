package com.example.exception;

public class UnsupportedMessageImplementation extends RuntimeException {

    public UnsupportedMessageImplementation() {
    }

    public UnsupportedMessageImplementation(String message) {
        super(message);
    }

    public UnsupportedMessageImplementation(String message, Throwable cause) {
        super(message, cause);
    }
}
