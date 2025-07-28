package github.luckygc.pgq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("工具类测试")
class UtilsTest {

    @Test
    @DisplayName("当消息列表为null时应该抛出异常")
    void shouldThrowExceptionWhenMessagesIsNull() {
        assertThatThrownBy(() -> Utils.checkMessagesNotEmpty(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");
    }

    @Test
    @DisplayName("当消息列表为空时应该抛出异常")
    void shouldThrowExceptionWhenMessagesIsEmpty() {
        List<String> emptyList = Collections.emptyList();

        assertThatThrownBy(() -> Utils.checkMessagesNotEmpty(emptyList))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("messages不能为空");
    }

    @Test
    @DisplayName("当消息列表不为空时不应该抛出异常")
    void shouldNotThrowExceptionWhenMessagesIsNotEmpty() {
        List<String> messages = Arrays.asList("message1", "message2");

        // 不应该抛出异常
        Utils.checkMessagesNotEmpty(messages);
    }

    @Test
    @DisplayName("当持续时间为负数时应该抛出异常")
    void shouldThrowExceptionWhenDurationIsNegative() {
        Duration negativeDuration = Duration.ofSeconds(-1);

        assertThatThrownBy(() -> Utils.checkDurationIsPositive(negativeDuration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration必须大于0秒");
    }

    @Test
    @DisplayName("当持续时间为零时应该抛出异常")
    void shouldThrowExceptionWhenDurationIsZero() {
        Duration zeroDuration = Duration.ZERO;

        assertThatThrownBy(() -> Utils.checkDurationIsPositive(zeroDuration))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("duration必须大于0秒");
    }

    @Test
    @DisplayName("当持续时间为正数时不应该抛出异常")
    void shouldNotThrowExceptionWhenDurationIsPositive() {
        Duration positiveDuration = Duration.ofSeconds(1);

        // 不应该抛出异常
        Utils.checkDurationIsPositive(positiveDuration);
    }

    @Test
    @DisplayName("应该正确提取消息ID数组")
    void shouldExtractMessageIdsCorrectly() {
        // 创建测试消息
        Message message1 = Message.of("topic1", "payload1", 1);
        Message message2 = Message.of("topic2", "payload2", 2);
        Message message3 = Message.of("topic3", "payload3", 3);

        // 手动设置ID（通过反射或创建带ID的消息）
        setMessageId(message1, 100L);
        setMessageId(message2, 200L);
        setMessageId(message3, 300L);

        List<Message> messages = Arrays.asList(message1, message2, message3);

        Long[] ids = Utils.getIdArray(messages);

        assertThat(ids).hasSize(3);
        assertThat(ids[0]).isEqualTo(100L);
        assertThat(ids[1]).isEqualTo(200L);
        assertThat(ids[2]).isEqualTo(300L);
    }

    @Test
    @DisplayName("空消息列表应该返回空数组")
    void shouldReturnEmptyArrayForEmptyMessageList() {
        List<Message> emptyMessages = new ArrayList<>();

        Long[] ids = Utils.getIdArray(emptyMessages);

        assertThat(ids).isEmpty();
    }

    @Test
    @DisplayName("当消息ID为null时应该抛出异常")
    void shouldThrowExceptionWhenMessageIdIsNull() {
        Message messageWithNullId = Message.of("topic", "payload", 1);
        // ID默认为null，不需要设置

        List<Message> messages = Arrays.asList(messageWithNullId);

        assertThatThrownBy(() -> Utils.getIdArray(messages))
                .isInstanceOf(NullPointerException.class);
    }

    // 辅助方法：通过反射设置Message的ID
    private void setMessageId(Message message, Long id) {
        try {
            java.lang.reflect.Field idField = Message.class.getDeclaredField("id");
            idField.setAccessible(true);
            idField.set(message, id);
        } catch (Exception e) {
            throw new RuntimeException("无法设置消息ID", e);
        }
    }
} 
