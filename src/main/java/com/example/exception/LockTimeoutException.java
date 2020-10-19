package com.example.exception;

public class LockTimeoutException extends RuntimeException {

    public LockTimeoutException() {
    }

    public LockTimeoutException(final String message) {
        super(message);
    }

    public LockTimeoutException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
