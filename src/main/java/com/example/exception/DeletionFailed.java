package com.example.exception;

public class DeletionFailed extends RuntimeException {

    public DeletionFailed() {
    }

    public DeletionFailed(String message) {
        super(message);
    }

    public DeletionFailed(String message, Throwable cause) {
        super(message, cause);
    }
}
