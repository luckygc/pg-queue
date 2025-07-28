package github.luckygc.pgq.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("消息模型测试")
class MessageTest {

    @Test
    @DisplayName("应该正确创建单个消息")
    void shouldCreateSingleMessageCorrectly() {
        String topic = "test-topic";
        String payload = "test-message";
        int priority = 5;

        Message message = Message.of(topic, payload, priority);

        assertThat(message.getTopic()).isEqualTo(topic);
        assertThat(message.getPayload()).isEqualTo(payload);
        assertThat(message.getPriority()).isEqualTo(priority);
        assertThat(message.getAttempt()).isEqualTo(0);
        assertThat(message.getCreateTime()).isNotNull();
        assertThat(message.getId()).isNull(); // ID在创建时为null，由数据库生成
    }

    @Test
    @DisplayName("创建时间应该接近当前时间")
    void shouldSetCreateTimeToNow() {
        LocalDateTime before = LocalDateTime.now();
        Message message = Message.of("topic", "payload", 1);
        LocalDateTime after = LocalDateTime.now();

        assertThat(message.getCreateTime())
                .isNotNull()
                .isAfterOrEqualTo(before)
                .isBeforeOrEqualTo(after);
    }

    @Test
    @DisplayName("当topic为null时应该抛出异常")
    void shouldThrowExceptionWhenTopicIsNull() {
        assertThatThrownBy(() -> Message.of(null, "payload", 1))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("当payload为null时应该抛出异常")
    void shouldThrowExceptionWhenPayloadIsNull() {
        assertThatThrownBy(() -> Message.of("topic", (String) null, 1))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("应该正确创建批量消息")
    void shouldCreateBatchMessagesCorrectly() {
        String topic = "test-topic";
        List<String> payloads = Arrays.asList("message1", "message2", "message3");
        int priority = 3;

        List<Message> messages = Message.of(topic, payloads, priority);

        assertThat(messages).hasSize(3);

        for (int i = 0; i < messages.size(); i++) {
            Message message = messages.get(i);
            assertThat(message.getTopic()).isEqualTo(topic);
            assertThat(message.getPayload()).isEqualTo(payloads.get(i));
            assertThat(message.getPriority()).isEqualTo(priority);
            assertThat(message.getAttempt()).isEqualTo(0);
            assertThat(message.getCreateTime()).isNotNull();
            assertThat(message.getId()).isNull();
        }
    }

    @Test
    @DisplayName("批量消息应该具有相同的创建时间")
    void shouldHaveSameCreateTimeForBatchMessages() {
        String topic = "test-topic";
        List<String> payloads = Arrays.asList("message1", "message2", "message3");
        int priority = 1;

        List<Message> messages = Message.of(topic, payloads, priority);

        LocalDateTime firstCreateTime = messages.get(0).getCreateTime();
        for (Message message : messages) {
            assertThat(message.getCreateTime()).isEqualTo(firstCreateTime);
        }
    }

    @Test
    @DisplayName("当topic为null时应该抛出异常（批量创建）")
    void shouldThrowExceptionWhenTopicIsNullInBatch() {
        List<String> payloads = Arrays.asList("message1", "message2");

        assertThatThrownBy(() -> Message.of(null, payloads, 1))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("当消息列表为null时应该抛出异常")
    void shouldThrowExceptionWhenPayloadsIsNull() {
        assertThatThrownBy(() -> Message.of("topic", (List<String>) null, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");
    }

    @Test
    @DisplayName("当消息列表为空时应该抛出异常")
    void shouldThrowExceptionWhenPayloadsIsEmpty() {
        List<String> emptyPayloads = Collections.emptyList();

        assertThatThrownBy(() -> Message.of("topic", emptyPayloads, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");
    }

    @Test
    @DisplayName("应该正确处理单个消息的批量创建")
    void shouldHandleSingleMessageInBatch() {
        String topic = "test-topic";
        List<String> singlePayload = Arrays.asList("single-message");
        int priority = 2;

        List<Message> messages = Message.of(topic, singlePayload, priority);

        assertThat(messages).hasSize(1);
        Message message = messages.get(0);
        assertThat(message.getTopic()).isEqualTo(topic);
        assertThat(message.getPayload()).isEqualTo("single-message");
        assertThat(message.getPriority()).isEqualTo(priority);
    }

    @Test
    @DisplayName("新创建的消息应该有正确的默认值")
    void shouldHaveCorrectDefaultValues() {
        Message message = Message.of("topic", "payload", 10);

        assertThat(message.getId()).isNull();
        assertThat(message.getTopic()).isEqualTo("topic");
        assertThat(message.getPayload()).isEqualTo("payload");
        assertThat(message.getPriority()).isEqualTo(10);
        assertThat(message.getAttempt()).isEqualTo(0);
        assertThat(message.getCreateTime()).isNotNull();
    }

    @Test
    @DisplayName("应该正确处理负优先级")
    void shouldHandleNegativePriority() {
        Message message = Message.of("topic", "payload", -5);

        assertThat(message.getPriority()).isEqualTo(-5);
    }

    @Test
    @DisplayName("应该正确处理零优先级")
    void shouldHandleZeroPriority() {
        Message message = Message.of("topic", "payload", 0);

        assertThat(message.getPriority()).isEqualTo(0);
    }

    @Test
    @DisplayName("应该正确处理空字符串payload")
    void shouldHandleEmptyStringPayload() {
        Message message = Message.of("topic", "", 1);

        assertThat(message.getPayload()).isEqualTo("");
    }

    @Test
    @DisplayName("应该正确处理空字符串topic")
    void shouldHandleEmptyStringTopic() {
        Message message = Message.of("", "payload", 1);

        assertThat(message.getTopic()).isEqualTo("");
    }
} 
