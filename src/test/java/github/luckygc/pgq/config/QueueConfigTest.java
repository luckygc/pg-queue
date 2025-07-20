package github.luckygc.pgq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import github.luckygc.pgq.api.MessageHandler;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("队列配置测试")
public class QueueConfigTest {

    private static class TestMessage {

    }

    private final MessageHandler<TestMessage> mockHandler = new MessageHandler<TestMessage>() {
        @Override
        public Class<TestMessage> getMessageCls() {
            return TestMessage.class;
        }

        @Override
        public boolean handle(TestMessage message) {
            return true;
        }
    };

    @Test
    @DisplayName("使用Builder创建完整配置")
    void testBuilderWithAllParameters() {
        // Given
        String topic = "test-topic";
        int maxAttempt = 3;
        Duration firstDelay = Duration.ofSeconds(30);
        Duration nextDelay = Duration.ofMinutes(5);
        int handlerCount = 2;
        Duration retentionTime = Duration.ofHours(12);

        // When
        QueueConfig<TestMessage> config = new QueueConfig.Builder<TestMessage>().topic(topic).maxAttempt(maxAttempt)
                .firstProcessDelay(firstDelay)
                .nextProcessDelay(nextDelay).messageHandler(mockHandler).handlerCount(handlerCount)
                .retentionTime(retentionTime).build();

        // Then
        assertThat(config.getTopic()).isEqualTo(topic);
        assertThat(config.getMaxAttempt()).isEqualTo(maxAttempt);
        assertThat(config.getFirstProcessDelay()).isPresent();
        assertThat(config.getFirstProcessDelay().get()).isEqualTo(firstDelay);
        assertThat(config.getNextProcessDelay()).isEqualTo(nextDelay);
        assertThat(config.getMessageHandler()).isEqualTo(mockHandler);
        assertThat(config.getHandlerCount()).isEqualTo(handlerCount);
        assertThat(config.getRetentionTime()).isEqualTo(retentionTime);
    }

    @Test
    @DisplayName("使用Builder创建最小配置")
    void testBuilderWithMinimalParameters() {
        // Given
        String topic = "minimal-topic";

        // When
        QueueConfig<TestMessage> config = new QueueConfig.Builder<TestMessage>().topic(topic)
                .messageHandler(mockHandler).build();

        // Then
        assertThat(config.getTopic()).isEqualTo(topic);
        assertThat(config.getMaxAttempt()).isEqualTo(1); // 默认值
        assertThat(config.getFirstProcessDelay()).isNotPresent(); // 默认无延迟
        assertThat(config.getNextProcessDelay()).isEqualTo(Duration.ofMinutes(10)); // 默认值
        assertThat(config.getMessageHandler()).isEqualTo(mockHandler);
        assertThat(config.getHandlerCount()).isEqualTo(1); // 默认值
        assertThat(config.getRetentionTime()).isEqualTo(Duration.ofDays(1)); // 默认值
    }

    @Test
    @DisplayName("topic为空时抛出异常")
    void testEmptyTopicThrowsException() {
        // When & Then
        assertThatThrownBy(() -> new QueueConfig.Builder<TestMessage>().topic("").messageHandler(mockHandler)
                .build()).isInstanceOf(
                IllegalArgumentException.class).hasMessage("topic不能为空");
    }

    @Test
    @DisplayName("topic为null时抛出异常")
    void testNullTopicThrowsException() {
        // When & Then
        assertThatThrownBy(
                () -> new QueueConfig.Builder<TestMessage>().topic(null).messageHandler(mockHandler)
                        .build()).isInstanceOf(
                IllegalArgumentException.class).hasMessage("topic不能为空");
    }

    @Test
    @DisplayName("maxAttempt小于1时抛出异常")
    void testInvalidMaxAttemptThrowsException() {
        // When & Then
        assertThatThrownBy(() -> new QueueConfig.Builder<TestMessage>().topic("test-topic").maxAttempt(0)
                .messageHandler(mockHandler)
                .build()).isInstanceOf(IllegalArgumentException.class).hasMessage("maxAttempt不能小于1");
    }

    @Test
    @DisplayName("messageHandler为null时抛出异常")
    void testNullMessageHandlerThrowsException() {
        // When & Then
        assertThatThrownBy(
                () -> new QueueConfig.Builder<TestMessage>().topic("test-topic").messageHandler(null)
                        .build()).isInstanceOf(
                IllegalArgumentException.class).hasMessage("messageHandler不能为null");
    }

    @Test
    @DisplayName("handlerCount小于1时抛出异常")
    void testInvalidHandlerCountThrowsException() {
        // When & Then
        assertThatThrownBy(() -> new QueueConfig.Builder<TestMessage>()
                .topic("test-topic")
                .messageHandler(mockHandler)
                .handlerCount(0)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("handlerCount不能小于1");
    }

    @Test
    @DisplayName("测试firstProcessDelay为null时的Optional处理")
    void testOptionalFirstProcessDelay() {
        // When
        QueueConfig<TestMessage> config = new QueueConfig.Builder<TestMessage>().topic("optional-test")
                .messageHandler(mockHandler).build();

        // Then
        assertThat(config.getFirstProcessDelay()).isNotPresent();
    }

    @Test
    @DisplayName("测试自定义retentionTime")
    void testCustomRetentionTime() {
        // Given
        Duration customRetentionTime = Duration.ofHours(6);

        // When
        QueueConfig<TestMessage> config = new QueueConfig.Builder<TestMessage>()
                .topic("retention-test")
                .messageHandler(mockHandler)
                .retentionTime(customRetentionTime)
                .build();

        // Then
        assertThat(config.getRetentionTime()).isEqualTo(customRetentionTime);
    }

    @Test
    @DisplayName("测试retentionTime默认值")
    void testDefaultRetentionTime() {
        // When
        QueueConfig<TestMessage> config = new QueueConfig.Builder<TestMessage>()
                .topic("default-retention-test")
                .messageHandler(mockHandler)
                .build();

        // Then
        assertThat(config.getRetentionTime()).isEqualTo(Duration.ofDays(1));
    }

    @Test
    @DisplayName("测试所有getter方法")
    void testAllGetters() {
        // Given
        String topic = "getter-test";
        int maxAttempt = 5;
        Duration firstDelay = Duration.ofMinutes(1);
        Duration nextDelay = Duration.ofMinutes(15);
        int handlerCount = 4;
        Duration retentionTime = Duration.ofHours(8);

        // When
        QueueConfig<TestMessage> config = new QueueConfig.Builder<TestMessage>().topic(topic).maxAttempt(maxAttempt)
                .firstProcessDelay(firstDelay)
                .nextProcessDelay(nextDelay).messageHandler(mockHandler).handlerCount(handlerCount)
                .retentionTime(retentionTime).build();

        // Then
        assertThat(config).extracting(QueueConfig::getTopic, QueueConfig::getMaxAttempt,
                        c -> c.getFirstProcessDelay().orElse(null), QueueConfig::getNextProcessDelay,
                        QueueConfig::getMessageHandler, QueueConfig::getHandlerCount, QueueConfig::getRetentionTime)
                .containsExactly(topic, maxAttempt, firstDelay, nextDelay, mockHandler, handlerCount, retentionTime);
    }
}
