package com.example;

/**
 * Provides generic way to access underlying queue without reliance on implementation specific.
 *
 * @param <T> message type supported by queue
 */
public interface QueueService<T> {

    /**
     * Pushes message to specified queue. Messages are stored in FIFO manner. The implementor might
     * choose if duplications are allowed.
     *
     * @param queue   to push message to
     * @param message string body of a message
     */
    void push(final String queue, String message);

    /**
     * @param queue to pull message from
     * @return fetched message object
     */
    T pull(final String queue);

    /**
     * Removes specified message from a specified queue. Implementor must specify the way messages are
     * distinguished from one another.
     *
     * @param queue   to delete messages from
     * @param message to delete
     */
    void delete(final String queue, T message);
}