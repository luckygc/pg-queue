package github.luckygc.pgq.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import github.luckygc.pgq.dao.MessageDao;
import java.time.LocalDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessageTest {

    @Mock
    private MessageDao messageDao;

    private Message message;
    private LocalDateTime createTime;

    @BeforeEach
    void setUp() {
        createTime = LocalDateTime.now();
        message = new Message.Builder()
                .id(1L)
                .createTime(createTime)
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .messageDao(messageDao)
                .build();
    }

    @Test
    void shouldCreateMessageWithAllFields() {
        assertThat(message.getCreateTime()).isEqualTo(createTime);
        assertThat(message.getTopic()).isEqualTo("test-topic");
        assertThat(message.getPriority()).isEqualTo(5);
        assertThat(message.getPayload()).isEqualTo("test-payload");
        assertThat(message.getAttempt()).isEqualTo(1);
    }

    @Test
    void shouldThrowExceptionWhenIdIsNull() {
        assertThatThrownBy(() -> new Message.Builder()
                .createTime(createTime)
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .messageDao(messageDao)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenCreateTimeIsNull() {
        assertThatThrownBy(() -> new Message.Builder()
                .id(1L)
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .messageDao(messageDao)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenTopicIsNull() {
        assertThatThrownBy(() -> new Message.Builder()
                .id(1L)
                .createTime(createTime)
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .messageDao(messageDao)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenPayloadIsNull() {
        assertThatThrownBy(() -> new Message.Builder()
                .id(1L)
                .createTime(createTime)
                .topic("test-topic")
                .priority(5)
                .attempt(1)
                .messageDao(messageDao)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenMessageDaoIsNull() {
        assertThatThrownBy(() -> new Message.Builder()
                .id(1L)
                .createTime(createTime)
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldDeleteMessageSuccessfully() {
        when(messageDao.deleteProcessingMessageById(1L)).thenReturn(1);

        message.delete();

        verify(messageDao).deleteProcessingMessageById(1L);
    }

    @Test
    void shouldMoveMessageToDeadQueue() {
        when(messageDao.moveProcessingMessageToDeadById(1L)).thenReturn(1);

        message.dead();

        verify(messageDao).moveProcessingMessageToDeadById(1L);
    }

    @Test
    void shouldRetryMessage() {
        when(messageDao.moveProcessingMessageToPendingById(1L)).thenReturn(1);

        message.retry();

        verify(messageDao).moveProcessingMessageToPendingById(1L);
    }

    @Test
    void shouldLogWarningWhenDeleteFails() {
        when(messageDao.deleteProcessingMessageById(1L)).thenReturn(0);

        // 这里不会抛出异常，只是记录警告日志
        message.delete();

        verify(messageDao).deleteProcessingMessageById(1L);
    }

    @Test
    void shouldLogWarningWhenDeadFails() {
        when(messageDao.moveProcessingMessageToDeadById(1L)).thenReturn(0);

        // 这里不会抛出异常，只是记录警告日志
        message.dead();

        verify(messageDao).moveProcessingMessageToDeadById(1L);
    }

    @Test
    void shouldLogWarningWhenRetryFails() {
        when(messageDao.moveProcessingMessageToPendingById(1L)).thenReturn(0);

        // 这里不会抛出异常，只是记录警告日志
        message.retry();

        verify(messageDao).moveProcessingMessageToPendingById(1L);
    }
}
