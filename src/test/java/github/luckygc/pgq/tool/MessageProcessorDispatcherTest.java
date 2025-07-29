package github.luckygc.pgq.tool;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import github.luckygc.pgq.api.MessageProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@ExtendWith(MockitoExtension.class)
class MessageProcessorDispatcherTest {

    @Mock
    private MessageProcessor messageProcessor1;

    @Mock
    private MessageProcessor messageProcessor2;

    private MessageProcessorDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new MessageProcessorDispatcher();

        // 清理事务同步管理器状态
        TransactionSynchronizationManager.clear();
    }

    @Test
    void shouldRegisterMessageProcessor() {
        when(messageProcessor1.topic()).thenReturn("topic1");

        dispatcher.register(messageProcessor1);

        // 验证注册成功，通过调度来验证
        dispatcher.dispatch("topic1");
        verify(messageProcessor1).asyncProcess();
    }

    @Test
    void shouldThrowExceptionWhenRegisteringDuplicateTopic() {
        when(messageProcessor1.topic()).thenReturn("duplicate-topic");
        when(messageProcessor2.topic()).thenReturn("duplicate-topic");

        dispatcher.register(messageProcessor1);

        assertThatThrownBy(() -> dispatcher.register(messageProcessor2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("当前已存在topic[duplicate-topic]的消息处理器");
    }

    @Test
    void shouldDispatchToCorrectProcessor() {
        when(messageProcessor1.topic()).thenReturn("topic1");
        when(messageProcessor2.topic()).thenReturn("topic2");

        dispatcher.register(messageProcessor1);
        dispatcher.register(messageProcessor2);

        dispatcher.dispatch("topic1");
        verify(messageProcessor1).asyncProcess();
        verify(messageProcessor2, never()).asyncProcess();

        dispatcher.dispatch("topic2");
        verify(messageProcessor2).asyncProcess();
        verify(messageProcessor1, times(1)).asyncProcess(); // 仍然只调用了一次
    }

    @Test
    void shouldIgnoreDispatchForUnknownTopic() {
        when(messageProcessor1.topic()).thenReturn("known-topic");
        dispatcher.register(messageProcessor1);

        // 调度未知topic，不应该抛出异常
        dispatcher.dispatch("unknown-topic");

        verify(messageProcessor1, never()).asyncProcess();
    }

    @Test
    void shouldThrowNpeWithNullTopic() {
        assertThatThrownBy(() -> dispatcher.dispatch(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldShutdownAllProcessors() {
        when(messageProcessor1.topic()).thenReturn("topic1");
        when(messageProcessor2.topic()).thenReturn("topic2");

        dispatcher.register(messageProcessor1);
        dispatcher.register(messageProcessor2);

        dispatcher.shutdown();

        verify(messageProcessor1).shutdown();
        verify(messageProcessor2).shutdown();
    }

    @Test
    void shouldHandleShutdownWhenNoProcessors() {
        // 没有注册任何处理器时调用shutdown，不应该抛出异常
        dispatcher.shutdown();
    }

    @Test
    void shouldRegisterMultipleProcessorsWithDifferentTopics() {
        when(messageProcessor1.topic()).thenReturn("topic1");
        when(messageProcessor2.topic()).thenReturn("topic2");

        dispatcher.register(messageProcessor1);
        dispatcher.register(messageProcessor2);

        // 验证两个处理器都能正常工作
        dispatcher.dispatch("topic1");
        dispatcher.dispatch("topic2");

        verify(messageProcessor1).asyncProcess();
        verify(messageProcessor2).asyncProcess();
    }

    @Test
    void shouldHandleProcessorExceptionDuringDispatch() {
        when(messageProcessor1.topic()).thenReturn("error-topic");
        doThrow(new RuntimeException("Processing error")).when(messageProcessor1).asyncProcess();

        dispatcher.register(messageProcessor1);

        // 即使处理器抛出异常，调度器也不应该崩溃
        dispatcher.dispatch("error-topic");

        verify(messageProcessor1).asyncProcess();
    }

    @Test
    void shouldHandleProcessorExceptionDuringShutdown() {
        when(messageProcessor1.topic()).thenReturn("topic1");
        when(messageProcessor2.topic()).thenReturn("topic2");
        doThrow(new RuntimeException("Shutdown error")).when(messageProcessor1).shutdown();

        dispatcher.register(messageProcessor1);
        dispatcher.register(messageProcessor2);

        // 即使一个处理器关闭时抛出异常，其他处理器也应该正常关闭
        dispatcher.shutdown();

        verify(messageProcessor1).shutdown();
        verify(messageProcessor2).shutdown();
    }

    @Test
    void shouldHandleEmptyTopicString() {
        when(messageProcessor1.topic()).thenReturn("");
        dispatcher.register(messageProcessor1);

        dispatcher.dispatch("");
        verify(messageProcessor1).asyncProcess();
    }

    @Test
    void shouldHandleWhitespaceOnlyTopic() {
        when(messageProcessor1.topic()).thenReturn("   ");
        dispatcher.register(messageProcessor1);

        dispatcher.dispatch("   ");
        verify(messageProcessor1).asyncProcess();
    }

    @Test
    void shouldHandleSpecialCharactersInTopic() {
        String specialTopic = "topic-with_special.chars@123";
        when(messageProcessor1.topic()).thenReturn(specialTopic);
        dispatcher.register(messageProcessor1);

        dispatcher.dispatch(specialTopic);
        verify(messageProcessor1).asyncProcess();
    }

    @Test
    void shouldHandleUnicodeCharactersInTopic() {
        String unicodeTopic = "主题-测试-🚀";
        when(messageProcessor1.topic()).thenReturn(unicodeTopic);
        dispatcher.register(messageProcessor1);

        dispatcher.dispatch(unicodeTopic);
        verify(messageProcessor1).asyncProcess();
    }
}
