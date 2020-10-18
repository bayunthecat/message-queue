package com.example.exception;

public class RowMappingException extends RuntimeException {

    public RowMappingException() {
    }

    public RowMappingException(String message) {
        super(message);
    }

    public RowMappingException(String message, Throwable cause) {
        super(message, cause);
    }
}
