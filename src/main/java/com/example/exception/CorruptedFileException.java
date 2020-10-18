package com.example.exception;

public class CorruptedFileException extends RuntimeException {

    public CorruptedFileException() {
    }

    public CorruptedFileException(String message) {
        super(message);
    }

    public CorruptedFileException(String message, Throwable cause) {
        super(message, cause);
    }
}