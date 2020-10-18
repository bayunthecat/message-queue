package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.example.impl.SqsQueueService;
import com.example.model.impl.AmazonSqsMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SqsQueueServiceTest {

    @Mock
    private AmazonSQSClient mockClient;

    @InjectMocks
    private SqsQueueService unit;

    private final static String TEST_QUEUE = "test";

    @Test
    void testPush() {
        final String message = UUID.randomUUID().toString();
        unit.push(TEST_QUEUE, message);
        verify(mockClient).sendMessage(TEST_QUEUE, message);
        verifyNoMoreInteractions(mockClient);
    }

    @Test
    void testPull() {
        final String receiptHandle = UUID.randomUUID().toString();
        final String body = UUID.randomUUID().toString();
        createMessage(receiptHandle, body);
        final ReceiveMessageResult result = new ReceiveMessageResult();
        result.setMessages(List.of(createMessage(receiptHandle, body)));
        when(mockClient.receiveMessage(TEST_QUEUE)).thenReturn(result);
        final AmazonSqsMessage actualMessage = unit.pull(TEST_QUEUE);
        assertEquals(receiptHandle, actualMessage.getReceiptHandle());
        assertEquals(body, actualMessage.getPayload());
        verify(mockClient).receiveMessage(TEST_QUEUE);
        verifyNoMoreInteractions(mockClient);
    }

    private Message createMessage(String receiptHandle, String body) {
        final Message message = new Message();
        message.setReceiptHandle(receiptHandle);
        message.setBody(body);
        return message;
    }

    @Test
    void testDelete() {
        final String receiptHandle = UUID.randomUUID().toString();
        AmazonSqsMessage message = AmazonSqsMessage.builder()
                                                   .payload(UUID.randomUUID().toString())
                                                   .receiptHandle(receiptHandle)
                                                   .build();
        unit.delete(TEST_QUEUE, message);
        verify(mockClient).deleteMessage(TEST_QUEUE, receiptHandle);
        verifyNoMoreInteractions(mockClient);
    }
}
