package com.example.impl;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

import com.example.QueueService;
import com.example.exception.DeletionFailed;
import com.example.exception.InvalidMessageBodyContent;
import com.example.exception.UnableToAccessUnderlyingStore;
import com.example.model.Messages;
import com.example.model.impl.SimpleMessage;
import com.example.model.mapper.SimpleMessageMapper;
import org.apache.commons.lang3.StringUtils;

/**
 * File based implementation of a {@link QueueService}. Service relies on addition order to ensure
 * queue behavior. Supports millisecond visibility timeout. When message is pulled visibility
 * timeout starts, upon next pull if time is already expired and message is not deleted service
 * returns it instead in order of attempted pulls. Each queue is stored in a separate file, as well
 * as redelivery queue. Service offers thread and process safe operation as all file operation are
 * guarded with file locks. Service does not cleanup empty queue files.
 */
public class FileQueueService implements QueueService<SimpleMessage> {

    private static final int COMPACTION_BUFFER_SIZE = 1024;

    private final String storagePath;

    private final String fileFormat;

    private final String inProgressFileSuffix;

    private final long visibilityTimeout;

    private final SimpleMessageMapper mapper = new SimpleMessageMapper();

    /**
     * Constructor for {@link FileQueueService}
     *
     * @param visibilityTimeout    millisecond timeout for message re delivery if not deleted
     * @param fileFormat           effectively file suffix, that will be used as underlying storage
     * @param inProgressFileSuffix suffix for in progress queue is appended after the queue name, but
     *                             before file format
     */
    public FileQueueService(final long visibilityTimeout,
                            final String storagePath,
                            final String fileFormat,
                            final String inProgressFileSuffix) {
        this.storagePath = storagePath;
        this.fileFormat = fileFormat;
        this.inProgressFileSuffix = inProgressFileSuffix;
        this.visibilityTimeout = visibilityTimeout;
    }

    /**
     * Pushes message to specified queue. If queue does not exist it is created upon message addition.
     * Current implementation hold limitation for permitted message symbols as , is not allowed.
     *
     * @param queue   to push message to
     * @param message string body of a message
     */
    @Override
    public void push(String queue, String message) {
        validateMessage(message);
        final SimpleMessage messageObject = Messages.createMessage(message);
        try (FileChannel queueChannel = FileChannel.open(Paths.get(getFileName(queue)), APPEND, CREATE)) {
            queueChannel.lock();
            push(queueChannel, messageObject);
        } catch (IOException e) {
            throw new UnableToAccessUnderlyingStore(e.getMessage(), e);
        }
    }

    /**
     * Pulls message from specified queue in order of addition. If queue is empty or does not exist
     * returns null.
     *
     * @param queue to pull message from
     * @return {@link SimpleMessage}
     */
    @Override
    public SimpleMessage pull(String queue) {
        try (FileChannel queueChannel = FileChannel.open(Paths.get(getFileName(queue)), READ, WRITE);
             FileChannel inProgressQueueChannel = FileChannel
                     .open(Paths.get(getInProgressFileName(queue)), READ, WRITE, CREATE)) {
            queueChannel.lock();
            inProgressQueueChannel.lock();
            final SimpleMessage inProgress = pollIf(inProgressQueueChannel,
                                                    message -> Messages.isExpired(message, visibilityTimeout),
                                                    message -> push(inProgressQueueChannel,
                                                                    Messages.createMessage(message)));
            if (Objects.nonNull(inProgress)) {
                return inProgress;
            }
            final SimpleMessage nextMessage = pollIf(queueChannel,
                                                     message -> true,
                                                     message -> push(inProgressQueueChannel,
                                                                     Messages.createMessage(message)));
            if (Objects.nonNull(nextMessage)) {
                return nextMessage;
            }
            return null;
        } catch (NoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw new UnableToAccessUnderlyingStore(e.getMessage(), e);
        }
    }

    /**
     * Removes message from specified queue. Also removes message from re-delivery queue.
     *
     * @param queue   to delete messages from
     * @param message to delete
     */
    @Override
    public void delete(final String queue, final SimpleMessage message) {
        try (FileChannel inProgressChannel = FileChannel.open(Paths.get(getInProgressFileName(queue)), READ, WRITE)) {
            inProgressChannel.lock();
            deleteIf(inProgressChannel, line -> {
                final SimpleMessage mapped = mapper.toMessage(line);
                return Objects.equals(message.getId(), mapped.getId());
            });
        } catch (IOException e) {
            throw new DeletionFailed(e.getMessage(), e);
        }
    }

    private void push(final FileChannel channel, final SimpleMessage message) {
        try {
            final String messageAsString = mapper.toString(message);
            channel.position(channel.size());
            channel.write(ByteBuffer.wrap(messageAsString.getBytes()));
        } catch (IOException e) {
            throw new UnableToAccessUnderlyingStore(e.getMessage(), e);
        }
    }

    private SimpleMessage pollIf(final FileChannel channel,
                                 final Predicate<SimpleMessage> predicate,
                                 final Consumer<SimpleMessage> onSuccessfulPoll) throws IOException {
        BufferedReader reader = new BufferedReader(
                Channels.newReader(channel, Charset.defaultCharset()));
        final String messageAsString = reader.lines().findFirst().orElse(null);
        if (Objects.nonNull(messageAsString)) {
            final SimpleMessage message = mapper.toMessage(messageAsString);
            if (predicate.test(message)) {
                compact(channel, 0, messageAsString.length() + System.lineSeparator().length());
                onSuccessfulPoll.accept(message);
                return message;
            }
        }
        return null;
    }

    private void compact(final FileChannel channel, long from, long to) throws IOException {
        long bytesToCopy = channel.size() - to;
        long bufferSize = COMPACTION_BUFFER_SIZE > bytesToCopy
                ? bytesToCopy
                : COMPACTION_BUFFER_SIZE;
        long copiedBytes = 0;
        long readPosition = to;
        long writePosition = from;
        do {
            ByteBuffer buffer = ByteBuffer.allocate((int) bufferSize);
            copiedBytes += channel.read(buffer, readPosition);
            readPosition += copiedBytes;
            writePosition += channel.write(buffer.flip(), writePosition);
        } while (bytesToCopy != copiedBytes);
        //have to trim file size accounting for deleted message
        channel.truncate(channel.size() - (to - from));
    }

    private void deleteIf(final FileChannel channel, final Predicate<String> predicate) throws IOException {
        BufferedReader reader = new BufferedReader(Channels.newReader(channel, Charset.defaultCharset()));
        long readBytes = 0;
        String currentLine;
        do {
            currentLine = reader.readLine();
            if (StringUtils.isNotEmpty(currentLine)) {
                readBytes += currentLine.length() + System.lineSeparator().length();
            }
        } while (Objects.nonNull(currentLine) && !predicate.test(currentLine));
        if (Objects.nonNull(currentLine)) {
            compact(channel, readBytes - (currentLine.length() + System.lineSeparator().length()), readBytes);
        }
    }

    private void validateMessage(final String message) {
        if (message.contains(System.lineSeparator()) || message.contains(",")) {
            throw new InvalidMessageBodyContent(
                    "Invalid message contents. Line separators and commas are not allowed.");
        }
    }

    private String getFileName(final String queueName) {
        return storagePath + queueName + fileFormat;
    }

    private String getInProgressFileName(final String queueName) {
        return storagePath + queueName + inProgressFileSuffix + fileFormat;
    }
}