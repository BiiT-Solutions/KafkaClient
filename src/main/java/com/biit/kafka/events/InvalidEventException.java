package com.biit.kafka.events;

public class InvalidEventException extends RuntimeException {

    public InvalidEventException(Class<?> clazz, String message) {
        super(clazz + ": " + message);
    }

    public InvalidEventException(Class<?> clazz, String message, Throwable e) {
        super(clazz + ": " + message, e);
    }

    public InvalidEventException(Class<?> clazz, Throwable e) {
        super(clazz.getName(), e);
    }
}
