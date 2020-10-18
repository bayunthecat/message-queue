package com.example;

import static java.nio.file.StandardOpenOption.READ;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.example.impl.FileQueueService;
import com.example.model.Message;
import com.example.model.impl.SimpleMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FileQueueTest {

    //Default storage at the root of a project
    private final String storage = "";

    private final String fileFormat = ".queue";

    private final String inProgressSuffix = "$";

    private String queue;

    private QueueService<SimpleMessage> queueService;

    private long visibilityTimeout = 500;

    @BeforeEach
    void setUp() {
        queue = UUID.randomUUID().toString();
        queueService = new FileQueueService(visibilityTimeout, storage, fileFormat, inProgressSuffix);
    }

    @AfterEach
    void tearDown() throws IOException {
        try {
            Files.delete(Paths.get(storage + queue + fileFormat));
            Files.delete(Paths.get(storage + queue + inProgressSuffix + fileFormat));
        } catch (NoSuchFileException e) {
            //Ignore exception, when file does not exist the goal is fulfilled
        }
    }

    @Test
    void testPush() {
        final List<String> expectedMessages = createMessages(2, queue);
        try (FileChannel queueChannel = FileChannel.open(Path.of(storage + queue + fileFormat), READ);
             BufferedReader reader = new BufferedReader(
                     Channels.newReader(queueChannel, Charset.defaultCharset()))) {
            final Object[] actualMessages = reader.lines().map(line -> line.split(",")[1]).toArray();
            assertArrayEquals(expectedMessages.toArray(), actualMessages);
        } catch (IOException e) {
            fail(e.getMessage(), e);
        }
    }

    @Test
    void testPoll() {
        final int messageNum = 3;
        final List<String> expectedMessages = createMessages(messageNum, queue);
        final List<SimpleMessage> actualMessages = IntStream.range(0, messageNum)
                                                            .mapToObj(sequence -> queueService.pull(queue))
                                                            .collect(Collectors.toList());
        assertArrayEquals(expectedMessages.toArray(),
                          actualMessages.stream().map(Message::getPayload).toArray());
    }

    @Test
    void testDelete() {
        createMessages(1, queue);
        final SimpleMessage message = queueService.pull(queue);
        queueService.delete(queue, message);
        final Message messageAfterDelete = queueService.pull(queue);
        assertNotNull(message);
        assertNull(messageAfterDelete);
    }

    @Test
    void testVisibilityTimeOut() throws InterruptedException {
        createMessages(1, queue);
        final Message messagePulledOnce = queueService.pull(queue);
        Thread.sleep(visibilityTimeout);
        final Message messagePulledAfterTimeout = queueService.pull(queue);
        assertEquals(messagePulledOnce.getPayload(), messagePulledAfterTimeout.getPayload());
    }

    @Test
    void testPollFromNonExistingQueue() {
        final Message actual = queueService.pull("non-existing-queue");
        assertNull(actual);
    }

    @Test
    void testPollFromEmptyQueue() throws InterruptedException {
        queueService.push(queue, UUID.randomUUID().toString());
        final SimpleMessage message = queueService.pull(queue);
        queueService.delete(queue, message);
        Thread.sleep(visibilityTimeout);
        final Message actualMessageFromEmptyQueue = queueService.pull(queue);
        assertNull(actualMessageFromEmptyQueue);
    }

    private List<String> createMessages(final int messageNum, final String targetQueue) {
        final List<String> expectedMessages = new ArrayList<>();
        IntStream.range(0, messageNum).forEach(sequence -> {
            final String message = UUID.randomUUID().toString();
            expectedMessages.add(message);
            queueService.push(targetQueue, message);
        });
        return expectedMessages;
    }
}
