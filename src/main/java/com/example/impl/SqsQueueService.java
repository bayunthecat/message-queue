package com.example.impl;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.example.QueueService;
import com.example.model.impl.AmazonSqsMessage;

/**
 * {@link QueueService} that uses AmazonSQS as underlying store.
 * <p>
 *
 * NOTE: I had no opportunity to test this solution since amazon account creation took much longer that expected.
 */
public class SqsQueueService implements QueueService<AmazonSqsMessage> {

    private final AmazonSQSClient sqsClient;

    public SqsQueueService(AmazonSQSClient sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * Pushes message to specified queue. It is supposed that queue is created and configured before
     * message is pushed since it has little sense to try and create specified queue on each request.
     *
     * @param queue   to push message to
     * @param message string body of a message
     */
    @Override
    public void push(String queue, String message) {
        sqsClient.sendMessage(queue, message);
    }

    /**
     * Pulls message from specified queue. Returns internal implementation of {@link
     * com.example.model.Message} interface. Visibility timeout is supported for queue creation ar
     * amazon CLI or service control panel.
     *
     * @param queue to pull message from
     * @return {@link AmazonSqsMessage} that contains encapsulated information necessary for deletion.
     */
    @Override
    public AmazonSqsMessage pull(String queue) {
        final ReceiveMessageResult result = sqsClient.receiveMessage(queue);
        return result.getMessages().stream().findFirst().map(
                message -> AmazonSqsMessage.builder()
                                           .receiptHandle(message.getReceiptHandle())
                                           .payload(message.getBody())
                                           .build()).orElse(null);
    }

    /**
     * Removes message from specified queue. In order for remove to work message has to pulled by
     * using {@link SqsQueueService#pull(String)}.
     *
     * @param queue   to delete messages from
     * @param message to delete
     */
    @Override
    public void delete(String queue, AmazonSqsMessage message) {
        sqsClient.deleteMessage(queue, message.getReceiptHandle());
    }
}
