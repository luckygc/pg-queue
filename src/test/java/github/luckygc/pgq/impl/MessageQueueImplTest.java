package github.luckygc.pgq.impl;

import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.model.MessageDO;
import github.luckygc.pgq.model.PgmqConstants;
import github.luckygc.pgq.tool.MessageProcessorDispatcher;
import github.luckygc.pgq.tool.PgNotifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class MessageQueueImplTest {

    @Mock
    private MessageDao messageDao;

    @Mock
    private MessageProcessorDispatcher dispatcher;

    @Mock
    private PgNotifier pgNotifier;

    @Mock
    private Message message;

    private MessageQueueImpl messageQueue;

    @BeforeEach
    void setUp() {
        messageQueue = new MessageQueueImpl(messageDao, dispatcher, pgNotifier);
    }

    @Test
    void shouldSendSingleMessage() {
        String topic = "test-topic";
        String payload = "test message";

        messageQueue.send(topic, payload);

        ArgumentCaptor<MessageDO> messageCaptor = ArgumentCaptor.forClass(MessageDO.class);
        verify(messageDao).insertIntoPending(messageCaptor.capture());
        verify(pgNotifier).sendNotify(topic);
        verify(dispatcher).dispatch(topic);

        MessageDO capturedMessage = messageCaptor.getValue();
        assertThat(capturedMessage.getTopic()).isEqualTo(topic);
        assertThat(capturedMessage.getPayload()).isEqualTo(payload);
        assertThat(capturedMessage.getPriority()).isEqualTo(PgmqConstants.MESSAGE_PRIORITY);
        assertThat(capturedMessage.getAttempt()).isEqualTo(0);
    }

    @Test
    void shouldSendSingleMessageWithoutNotifier() {
        MessageQueueImpl queueWithoutNotifier = new MessageQueueImpl(messageDao, dispatcher, null);
        String topic = "test-topic";
        String payload = "test message";

        queueWithoutNotifier.send(topic, payload);

        verify(messageDao).insertIntoPending(any(MessageDO.class));
        verify(dispatcher).dispatch(topic);
        verifyNoInteractions(pgNotifier);
    }

    @Test
    void shouldSendMultipleMessages() {
        String topic = "test-topic";
        List<String> payloads = Arrays.asList("message1", "message2", "message3");

        messageQueue.send(topic, payloads);

        ArgumentCaptor<List<MessageDO>> messagesCaptor = ArgumentCaptor.forClass(List.class);
        verify(messageDao).insertIntoPending(messagesCaptor.capture());
        verify(pgNotifier).sendNotify(topic);
        verify(dispatcher).dispatch(topic);

        List<MessageDO> capturedMessages = messagesCaptor.getValue();
        assertThat(capturedMessages).hasSize(3);
        assertThat(capturedMessages.get(0).getPayload()).isEqualTo("message1");
        assertThat(capturedMessages.get(1).getPayload()).isEqualTo("message2");
        assertThat(capturedMessages.get(2).getPayload()).isEqualTo("message3");
    }

    @Test
    void shouldThrowExceptionWhenSendingEmptyMessageList() {
        String topic = "test-topic";
        List<String> emptyMessages = Collections.emptyList();

        assertThatThrownBy(() -> messageQueue.send(topic, emptyMessages))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");

        verifyNoInteractions(messageDao, pgNotifier, dispatcher);
    }

    @Test
    void shouldThrowExceptionWhenSendingNullMessageList() {
        String topic = "test-topic";

        assertThatThrownBy(() -> messageQueue.send(topic, (List<String>) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");

        verifyNoInteractions(messageDao, pgNotifier, dispatcher);
    }

    @Test
    void shouldSendDelayedMessage() {
        String topic = "test-topic";
        String payload = "delayed message";
        Duration delay = Duration.ofMinutes(5);

        messageQueue.send(topic, payload, delay);

        ArgumentCaptor<MessageDO> messageCaptor = ArgumentCaptor.forClass(MessageDO.class);
        ArgumentCaptor<LocalDateTime> visibleTimeCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(messageDao).insertIntoInvisible(messageCaptor.capture(), visibleTimeCaptor.capture());

        MessageDO capturedMessage = messageCaptor.getValue();
        assertThat(capturedMessage.getTopic()).isEqualTo(topic);
        assertThat(capturedMessage.getPayload()).isEqualTo(payload);

        LocalDateTime visibleTime = visibleTimeCaptor.getValue();
        assertThat(visibleTime).isAfter(LocalDateTime.now().plus(delay).minusSeconds(1));
        assertThat(visibleTime).isBefore(LocalDateTime.now().plus(delay).plusSeconds(1));

        verifyNoInteractions(pgNotifier, dispatcher);
    }

    @Test
    void shouldSendMultipleDelayedMessages() {
        String topic = "test-topic";
        List<String> payloads = Arrays.asList("delayed1", "delayed2");
        Duration delay = Duration.ofMinutes(10);

        messageQueue.send(topic, payloads, delay);

        ArgumentCaptor<List<MessageDO>> messagesCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<LocalDateTime> visibleTimeCaptor = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(messageDao).insertIntoInvisible(messagesCaptor.capture(), visibleTimeCaptor.capture());

        List<MessageDO> capturedMessages = messagesCaptor.getValue();
        assertThat(capturedMessages).hasSize(2);
        assertThat(capturedMessages.get(0).getPayload()).isEqualTo("delayed1");
        assertThat(capturedMessages.get(1).getPayload()).isEqualTo("delayed2");

        verifyNoInteractions(pgNotifier, dispatcher);
    }

    @Test
    void shouldThrowExceptionWhenDelayIsZero() {
        String topic = "test-topic";
        String payload = "message";
        Duration zeroDelay = Duration.ZERO;

        assertThatThrownBy(() -> messageQueue.send(topic, payload, zeroDelay))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration必须大于0秒");

        verifyNoInteractions(messageDao, pgNotifier, dispatcher);
    }

    @Test
    void shouldThrowExceptionWhenDelayIsNegative() {
        String topic = "test-topic";
        String payload = "message";
        Duration negativeDelay = Duration.ofMinutes(-1);

        assertThatThrownBy(() -> messageQueue.send(topic, payload, negativeDelay))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration必须大于0秒");

        verifyNoInteractions(messageDao, pgNotifier, dispatcher);
    }

    @Test
    void shouldSendPriorityMessage() {
        String topic = "test-topic";
        String payload = "priority message";
        int priority = 10;

        messageQueue.send(topic, payload, priority);

        ArgumentCaptor<MessageDO> messageCaptor = ArgumentCaptor.forClass(MessageDO.class);
        verify(messageDao).insertIntoPending(messageCaptor.capture());
        verify(pgNotifier).sendNotify(topic);
        verify(dispatcher).dispatch(topic);

        MessageDO capturedMessage = messageCaptor.getValue();
        assertThat(capturedMessage.getTopic()).isEqualTo(topic);
        assertThat(capturedMessage.getPayload()).isEqualTo(payload);
        assertThat(capturedMessage.getPriority()).isEqualTo(priority);
    }

    @Test
    void shouldSendMultiplePriorityMessages() {
        String topic = "test-topic";
        List<String> payloads = Arrays.asList("priority1", "priority2");
        int priority = 5;

        messageQueue.send(topic, payloads, priority);

        ArgumentCaptor<List<MessageDO>> messagesCaptor = ArgumentCaptor.forClass(List.class);
        verify(messageDao).insertIntoPending(messagesCaptor.capture());

        List<MessageDO> capturedMessages = messagesCaptor.getValue();
        assertThat(capturedMessages).hasSize(2);
        assertThat(capturedMessages.get(0).getPriority()).isEqualTo(priority);
        assertThat(capturedMessages.get(1).getPriority()).isEqualTo(priority);
    }

    @Test
    void shouldPollSingleMessage() {
        String topic = "test-topic";
        when(messageDao.getPendingMessagesAndMoveToProcessing(eq(topic), eq(1), any(LocalDateTime.class)))
                .thenReturn(Arrays.asList(message));

        Message result = messageQueue.poll(topic);

        assertThat(result).isEqualTo(message);
        verify(messageDao).getPendingMessagesAndMoveToProcessing(eq(topic), eq(1), any(LocalDateTime.class));
    }

    @Test
    void shouldReturnNullWhenNoMessagesAvailable() {
        String topic = "test-topic";
        when(messageDao.getPendingMessagesAndMoveToProcessing(eq(topic), eq(1), any(LocalDateTime.class)))
                .thenReturn(Collections.emptyList());

        Message result = messageQueue.poll(topic);

        assertThat(result).isNull();
    }

    @Test
    void shouldPollMultipleMessages() {
        String topic = "test-topic";
        int maxPoll = 5;
        List<Message> messages = Arrays.asList(message, message);
        when(messageDao.getPendingMessagesAndMoveToProcessing(eq(topic), eq(maxPoll), any(LocalDateTime.class)))
                .thenReturn(messages);

        List<Message> result = messageQueue.poll(topic, maxPoll);

        assertThat(result).isEqualTo(messages);
        verify(messageDao).getPendingMessagesAndMoveToProcessing(eq(topic), eq(maxPoll), any(LocalDateTime.class));
    }

    @Test
    void shouldThrowExceptionWhenMaxPollIsInvalid() {
        String topic = "test-topic";

        assertThatThrownBy(() -> messageQueue.poll(topic, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");

        assertThatThrownBy(() -> messageQueue.poll(topic, 5001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");

        verifyNoInteractions(messageDao);
    }
}
