package com.example.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.example.QueueService;
import com.example.exception.UnsupportedMessageImplementation;
import com.example.model.Message;
import com.example.model.Messages;
import com.example.model.impl.SimpleMessage;
import com.example.util.Queues;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Defines in memory implementation for {@link QueueService}. Uses {@link SimpleMessage} and {@link
 * java.util.PriorityQueue} as underlying storage. Implementation is thread safe on queue name
 * level. Service strives to achieve FIFO behavior by placing messages to the {@link
 * java.util.PriorityQueue} based on addition time, providing millisecond precision. In case
 * addition time is identical random tie breaker is used to decide which message will be delivered.
 * Service supports visibility timeout parameter in milliseconds. When message is pulled visibility
 * timer starts, upon next pull if there are non deleted messages with expired timeout service will
 * return them in FIFO order. Messages that are re-pulled will be scheduled for re-pull unless they
 * are deleted by recipient.
 */
public class InMemoryQueueService implements QueueService<SimpleMessage> {

    private final Map<String, Queue<SimpleMessage>> messageQueuesByTopic = new HashMap<>();

    private final Map<String, Queue<SimpleMessage>> messagesInProgressByTopic = new HashMap<>();

    private final LockingService lockingService = new LockingService();

    private final long visibilityTimeout;

    public InMemoryQueueService(long visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    /**
     * Pushes message to a specified queue. Queue is created when a new message is pushed.
     *
     * @param queue   to push message to
     * @param message string body of a message
     */
    @Override
    public void push(String queue, String message) {
        try {
            lockingService.lock(queue);
            push(messageQueuesByTopic, queue, Messages.createMessage(message));
        } finally {
            lockingService.unlock(queue);
        }
    }

    /**
     * Pulls message from specified queue. Service strives to maintain order by addition time with
     * millisecond precision. Return null if queue is empty or does not exist.
     *
     * @param queue to pull message from
     * @return {@link SimpleMessage}
     */
    @Override
    public SimpleMessage pull(String queue) {
        try {
            lockingService.lock(queue);
            final Queue<SimpleMessage> queueInProgress = messagesInProgressByTopic.get(queue);
            final SimpleMessage messagesInProgress = pullIf(queueInProgress,
                                                            message -> Messages.isExpired(message, visibilityTimeout));
            if (Objects.nonNull(messagesInProgress)) {
                push(messagesInProgressByTopic, queue, Messages.createMessage(messagesInProgress));
                return messagesInProgress;
            }
            final Queue<SimpleMessage> messages = messageQueuesByTopic.get(queue);
            final SimpleMessage nextMessage = pull(messages);
            if (Objects.nonNull(nextMessage)) {
                push(messagesInProgressByTopic, queue, Messages.createMessage(nextMessage));
                return nextMessage;
            }
            return null;
        } finally {
            lockingService.unlock(queue);
        }
    }

    /**
     * Deletes message from a specified queue. Particular implementation uses UUID as unique message
     * identifier. Identifier is encapsulated within pulled messages, so user has to provide message
     * he pulled from queue using {@link QueueService#pull(String)}.
     *
     * @param queue   to delete messages from
     * @param message to delete
     */
    @Override
    public void delete(final String queue, final SimpleMessage message) {
        final SimpleMessage simpleMessage = tryCast(message);
        try {
            lockingService.lock(queue);
            remove(messageQueuesByTopic, queue, simpleMessage);
            remove(messagesInProgressByTopic, queue, simpleMessage);
        } finally {
            lockingService.unlock(queue);
        }
    }

    private SimpleMessage pull(final Queue<SimpleMessage> queue) {
        return pullIf(queue, (message) -> true);
    }

    private SimpleMessage pullIf(final Queue<SimpleMessage> queue,
                                 final Predicate<SimpleMessage> predicate) {
        if (CollectionUtils.isNotEmpty(queue)) {
            final SimpleMessage message = queue.peek();
            if (predicate.test(message)) {
                return queue.poll();
            }
        }
        return null;
    }

    private void push(final Map<String, Queue<SimpleMessage>> messagesByTopic, final String topic,
                      final SimpleMessage message) {
        messagesByTopic.merge(topic, Queues.createPriorityQueue(message), (oldMessages, newMessages) -> {
            oldMessages.addAll(newMessages);
            return oldMessages;
        });
    }

    private void remove(final Map<String, Queue<SimpleMessage>> messages, final String queue,
                        final SimpleMessage message) {
        final Queue<SimpleMessage> queueMessages = messages.get(queue);
        if (CollectionUtils.isNotEmpty(queueMessages)) {
            queueMessages.remove(message);
        }
    }

    private SimpleMessage tryCast(final Message message) {
        if (message instanceof SimpleMessage) {
            return (SimpleMessage) message;
        }
        throw new UnsupportedMessageImplementation(
                "Unable to precess message of class " + message.getClass().getCanonicalName() + ", "
                        + SimpleMessage.class.getCanonicalName() + " supported.");
    }

    private static class LockingService {

        private final Map<String, Lock> locks = new ConcurrentHashMap<>();

        void lock(final String key) {
            final Lock lock = locks.compute(key, (lockKey, oldLock) -> Objects.nonNull(oldLock)
                    ? oldLock
                    : new ReentrantLock());
            lock.lock();
        }

        void unlock(final String key) {
            locks.computeIfPresent(key, (lockKey, oldLock) -> {
                oldLock.unlock();
                return null;
            });
        }
    }
}
