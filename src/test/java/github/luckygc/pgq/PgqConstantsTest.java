package github.luckygc.pgq;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("常量类测试")
class PgqConstantsTest {

    @Test
    @DisplayName("默认消息优先级应该为0")
    void shouldHaveCorrectDefaultMessagePriority() {
        assertThat(PgqConstants.MESSAGE_PRIORITY).isEqualTo(0);
    }

    @Test
    @DisplayName("默认消息处理超时时间应该为10分钟")
    void shouldHaveCorrectDefaultProcessTimeout() {
        Duration expectedTimeout = Duration.ofMinutes(10);

        assertThat(PgqConstants.PROCESS_TIMEOUT).isEqualTo(expectedTimeout);
        assertThat(PgqConstants.PROCESS_TIMEOUT.toMinutes()).isEqualTo(10);
        assertThat(PgqConstants.PROCESS_TIMEOUT.getSeconds()).isEqualTo(600);
    }

    @Test
    @DisplayName("消息处理器拉取数量应该为50")
    void shouldHaveCorrectMessageHandlerPullCount() {
        assertThat(PgqConstants.MESSAGE_HANDLER_PULL_COUNT).isEqualTo(50);
    }

    @Test
    @DisplayName("消息处理器线程数量应该为1")
    void shouldHaveCorrectMessageHandlerThreadCount() {
        assertThat(PgqConstants.MESSAGE_HANDLER_THREAD_COUNT).isEqualTo(1);
    }

    @Test
    @DisplayName("PGQ ID应该为199738")
    void shouldHaveCorrectPgqId() {
        assertThat(PgqConstants.PGQ_ID).isEqualTo(199738);
    }

    @Test
    @DisplayName("调度器ID应该为1")
    void shouldHaveCorrectSchedulerId() {
        assertThat(PgqConstants.SCHEDULER_ID).isEqualTo(1);
    }

    @Test
    @DisplayName("主题通道名称应该为pgq_topic_channel")
    void shouldHaveCorrectTopicChannelName() {
        assertThat(PgqConstants.TOPIC_CHANNEL).isEqualTo("pgq_topic_channel");
    }

    @Test
    @DisplayName("MESSAGE_PRIORITY应该是int类型且大于等于0")
    void shouldVerifyMessagePriorityType() {
        // 验证常量类型和范围
        assertThat(PgqConstants.MESSAGE_PRIORITY).isInstanceOf(Integer.class);
        assertThat(PgqConstants.MESSAGE_PRIORITY).isGreaterThanOrEqualTo(0);
    }

    @Test
    @DisplayName("PROCESS_TIMEOUT应该是Duration类型且为正值")
    void shouldVerifyProcessTimeoutType() {
        assertThat(PgqConstants.PROCESS_TIMEOUT).isInstanceOf(Duration.class);
        assertThat(PgqConstants.PROCESS_TIMEOUT.isNegative()).isFalse();
        assertThat(PgqConstants.PROCESS_TIMEOUT.isZero()).isFalse();
    }

    @Test
    @DisplayName("线程和拉取数量应该是正整数")
    void shouldVerifyCountsArePositive() {
        assertThat(PgqConstants.MESSAGE_HANDLER_PULL_COUNT).isPositive();
        assertThat(PgqConstants.MESSAGE_HANDLER_THREAD_COUNT).isPositive();
    }

    @Test
    @DisplayName("ID值应该是正整数")
    void shouldVerifyIdsArePositive() {
        assertThat(PgqConstants.PGQ_ID).isPositive();
        assertThat(PgqConstants.SCHEDULER_ID).isPositive();
    }

    @Test
    @DisplayName("TOPIC_CHANNEL应该是非空字符串")
    void shouldVerifyTopicChannelIsNotEmpty() {
        assertThat(PgqConstants.TOPIC_CHANNEL).isNotNull();
        assertThat(PgqConstants.TOPIC_CHANNEL).isNotEmpty();
        assertThat(PgqConstants.TOPIC_CHANNEL).isNotBlank();
    }

    @Test
    @DisplayName("处理器配置应该合理")
    void shouldHaveReasonableHandlerConfiguration() {
        // 验证拉取数量和线程数量的合理性
        assertThat(PgqConstants.MESSAGE_HANDLER_PULL_COUNT)
                .isGreaterThan(0)
                .isLessThanOrEqualTo(1000); // 合理的上限

        assertThat(PgqConstants.MESSAGE_HANDLER_THREAD_COUNT)
                .isGreaterThan(0)
                .isLessThanOrEqualTo(200); // 合理的线程数上限
    }

    @Test
    @DisplayName("超时时间应该在合理范围内")
    void shouldHaveReasonableTimeout() {
        // 验证超时时间的合理性：应该在1分钟到1小时之间
        assertThat(PgqConstants.PROCESS_TIMEOUT.toMinutes())
                .isGreaterThanOrEqualTo(1)
                .isLessThanOrEqualTo(60);
    }

    @Test
    @DisplayName("系统ID应该在合理范围内")
    void shouldHaveReasonableSystemIds() {
        // 验证系统ID的合理性
        assertThat(PgqConstants.PGQ_ID).isGreaterThan(0);
        assertThat(PgqConstants.SCHEDULER_ID).isGreaterThan(0);
    }
} 
