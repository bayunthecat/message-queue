package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import com.example.impl.InMemoryQueueService;
import com.example.model.Message;
import com.example.model.impl.SimpleMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class InMemoryQueueTest {

    private QueueService<SimpleMessage> unit;

    private static final String TEST_QUEUE = "testQueue";

    private int visibilityTimeout = 500;

    @BeforeEach
    void beforeEach() {
        unit = new InMemoryQueueService(visibilityTimeout);
    }

    @Test
    void testPushAndPull() {
        final int msgNum = 10;
        final List<String> expectedMessages = createMessages(msgNum);
        IntStream.range(0, msgNum).forEach(sequence -> {
            final Message message = unit.pull(TEST_QUEUE);
            assertTrue(expectedMessages.contains(message.getPayload()));
            Assertions.assertNotNull(message);
        });
    }

    @Test
    void testPull() {
        final String randomMessage = UUID.randomUUID().toString();
        unit.push(TEST_QUEUE, randomMessage);
        final Message actualMessage = unit.pull(TEST_QUEUE);
        Assertions.assertNotNull(actualMessage);
        Assertions.assertEquals(randomMessage, actualMessage.getPayload());
    }

    @Test
    void testPullForNonExistingQueue() {
        final Message actualMessage = unit.pull(TEST_QUEUE);
        Assertions.assertNull(actualMessage);
    }

    @Test
    void testVisibilityTimeout() throws InterruptedException {
        unit.push(TEST_QUEUE, UUID.randomUUID().toString());
        final Message message = unit.pull(TEST_QUEUE);
        assertNotNull(message);
        Thread.sleep(visibilityTimeout);
        final Message repeatedMessage = unit.pull(TEST_QUEUE);
        assertNotNull(repeatedMessage);
        assertEquals(message.getPayload(), repeatedMessage.getPayload());
    }

    @Test
    void testDelete() throws InterruptedException {
        unit.push(TEST_QUEUE, UUID.randomUUID().toString());
        final SimpleMessage actual = unit.pull(TEST_QUEUE);
        assertNotNull(actual);
        unit.delete(TEST_QUEUE, actual);
        //Have to make sure message is not in progress
        Thread.sleep(visibilityTimeout);
        final Message messageAfterDelete = unit.pull(TEST_QUEUE);
        assertNull(messageAfterDelete);
    }

    private List<String> createMessages(final int numMessages) {
        final List<String> expectedMessages = new ArrayList<>();
        IntStream.range(0, numMessages)
                 .forEach(sequence -> {
                     final String message = UUID.randomUUID().toString();
                     expectedMessages.add(message);
                     unit.push(TEST_QUEUE, message);
                 });
        return expectedMessages;
    }
}
