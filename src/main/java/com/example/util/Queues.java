package com.example.util;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

public class Queues {

    private Queues() {
    }

    public static <T> Queue<T> createPriorityBlockingQueue(final T message) {
        Queue<T> queue = new PriorityBlockingQueue<>();
        queue.offer(message);
        return queue;
    }

    public static <T> Queue<T> createPriorityQueue(final T message) {
        Queue<T> queue = new PriorityQueue<>();
        queue.offer(message);
        return queue;
    }
}
