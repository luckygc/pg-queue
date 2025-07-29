package github.luckygc.pgq.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.model.Message;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MessageProcessorTest {

    @Mock
    private MessagePoller messagePoller;

    @Mock
    private MessageHandler messageHandler;

    @Mock
    private Message message1;

    @Mock
    private Message message2;

    private MessageProcessor messageProcessor;

    @BeforeEach
    void setUp() {
        when(messageHandler.topic()).thenReturn("test-topic");
        when(messageHandler.maxPoll()).thenReturn(10);
        when(messageHandler.threadCount()).thenReturn(2);
        
        messageProcessor = new MessageProcessor(messagePoller, messageHandler);
    }

    @Test
    void shouldCreateMessageProcessorWithValidParameters() {
        assertThat(messageProcessor.topic()).isEqualTo("test-topic");
    }

    @Test
    void shouldThrowExceptionWhenMessagePollerIsNull() {
        assertThatThrownBy(() -> new MessageProcessor(null, messageHandler))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenMessageHandlerIsNull() {
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenTopicIsNull() {
        when(messageHandler.topic()).thenReturn(null);
        
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, messageHandler))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenMaxPollIsZero() {
        when(messageHandler.maxPoll()).thenReturn(0);
        
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, messageHandler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");
    }

    @Test
    void shouldThrowExceptionWhenMaxPollIsNegative() {
        when(messageHandler.maxPoll()).thenReturn(-1);
        
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, messageHandler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");
    }

    @Test
    void shouldThrowExceptionWhenMaxPollIsTooLarge() {
        when(messageHandler.maxPoll()).thenReturn(5001);
        
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, messageHandler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxPoll必须在1-5000之间");
    }

    @Test
    void shouldThrowExceptionWhenThreadCountIsZero() {
        when(messageHandler.threadCount()).thenReturn(0);
        
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, messageHandler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("threadCount必须在1-200之间");
    }

    @Test
    void shouldThrowExceptionWhenThreadCountIsNegative() {
        when(messageHandler.threadCount()).thenReturn(-1);
        
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, messageHandler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("threadCount必须在1-200之间");
    }

    @Test
    void shouldThrowExceptionWhenThreadCountIsTooLarge() {
        when(messageHandler.threadCount()).thenReturn(201);
        
        assertThatThrownBy(() -> new MessageProcessor(messagePoller, messageHandler))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("threadCount必须在1-200之间");
    }

    @Test
    void shouldAcceptValidMaxPollBoundaryValues() {
        when(messageHandler.maxPoll()).thenReturn(1);
        MessageProcessor processor1 = new MessageProcessor(messagePoller, messageHandler);
        assertThat(processor1.topic()).isEqualTo("test-topic");

        when(messageHandler.maxPoll()).thenReturn(5000);
        MessageProcessor processor2 = new MessageProcessor(messagePoller, messageHandler);
        assertThat(processor2.topic()).isEqualTo("test-topic");
    }

    @Test
    void shouldAcceptValidThreadCountBoundaryValues() {
        when(messageHandler.threadCount()).thenReturn(1);
        MessageProcessor processor1 = new MessageProcessor(messagePoller, messageHandler);
        assertThat(processor1.topic()).isEqualTo("test-topic");

        when(messageHandler.threadCount()).thenReturn(200);
        MessageProcessor processor2 = new MessageProcessor(messagePoller, messageHandler);
        assertThat(processor2.topic()).isEqualTo("test-topic");
    }

    @Test
    void shouldProcessMessagesWhenAsyncProcessIsCalled() throws InterruptedException {
        List<Message> messages = Arrays.asList(message1, message2);
        when(messagePoller.poll("test-topic", 10))
                .thenReturn(messages)
                .thenReturn(Collections.emptyList());

        CountDownLatch latch = new CountDownLatch(2);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(messageHandler).handle(any(Message.class));

        messageProcessor.asyncProcess();

        // 等待消息处理完成
        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        verify(messageHandler).handle(message1);
        verify(messageHandler).handle(message2);
    }

    @Test
    void shouldShutdownGracefully() {
        messageProcessor.shutdown();
        
        // 验证线程池已关闭
        // 这里我们无法直接验证线程池状态，但可以确保方法调用不抛出异常
    }
}
