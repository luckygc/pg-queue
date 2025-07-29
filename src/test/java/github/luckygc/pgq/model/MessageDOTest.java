package github.luckygc.pgq.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class MessageDOTest {

    @Test
    void shouldCreateMessageDOWithAllFields() {
        LocalDateTime createTime = LocalDateTime.now();

        MessageDO messageDO = MessageDO.Builder.create()
                .createTime(createTime)
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .build();

        assertThat(messageDO.getCreateTime()).isEqualTo(createTime);
        assertThat(messageDO.getTopic()).isEqualTo("test-topic");
        assertThat(messageDO.getPriority()).isEqualTo(5);
        assertThat(messageDO.getPayload()).isEqualTo("test-payload");
        assertThat(messageDO.getAttempt()).isEqualTo(1);
    }

    @Test
    void shouldUseCurrentTimeWhenCreateTimeIsNull() {
        LocalDateTime beforeCreation = LocalDateTime.now();

        MessageDO messageDO = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .build();

        LocalDateTime afterCreation = LocalDateTime.now();

        assertThat(messageDO.getCreateTime()).isBetween(beforeCreation, afterCreation);
        assertThat(messageDO.getTopic()).isEqualTo("test-topic");
        assertThat(messageDO.getPriority()).isEqualTo(5);
        assertThat(messageDO.getPayload()).isEqualTo("test-payload");
        assertThat(messageDO.getAttempt()).isEqualTo(1);
    }

    @Test
    void shouldThrowExceptionWhenTopicIsNull() {
        assertThatThrownBy(() -> MessageDO.Builder.create()
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenPriorityIsNull() {
        assertThatThrownBy(() -> MessageDO.Builder.create()
                .topic("test-topic")
                .payload("test-payload")
                .attempt(1)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenPayloadIsNull() {
        assertThatThrownBy(() -> MessageDO.Builder.create()
                .topic("test-topic")
                .priority(5)
                .attempt(1)
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowExceptionWhenAttemptIsNull() {
        assertThatThrownBy(() -> MessageDO.Builder.create()
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .build())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldCreateBuilderWithStaticMethod() {
        MessageDO.Builder builder = MessageDO.Builder.create();

        assertThat(builder).isNotNull();
    }

    @Test
    void shouldSupportMethodChaining() {
        LocalDateTime createTime = LocalDateTime.now();

        MessageDO messageDO = MessageDO.Builder.create()
                .createTime(createTime)
                .topic("test-topic")
                .priority(5)
                .payload("test-payload")
                .attempt(1)
                .build();

        assertThat(messageDO).isNotNull();
        assertThat(messageDO.getCreateTime()).isEqualTo(createTime);
        assertThat(messageDO.getTopic()).isEqualTo("test-topic");
        assertThat(messageDO.getPriority()).isEqualTo(5);
        assertThat(messageDO.getPayload()).isEqualTo("test-payload");
        assertThat(messageDO.getAttempt()).isEqualTo(1);
    }

    @Test
    void shouldHandleZeroPriority() {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(0)
                .payload("test-payload")
                .attempt(0)
                .build();

        assertThat(messageDO.getPriority()).isEqualTo(0);
        assertThat(messageDO.getAttempt()).isEqualTo(0);
    }

    @Test
    void shouldHandleNegativePriority() {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(-1)
                .payload("test-payload")
                .attempt(0)
                .build();

        assertThat(messageDO.getPriority()).isEqualTo(-1);
    }

    @Test
    void shouldHandleEmptyPayload() {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(0)
                .payload("")
                .attempt(0)
                .build();

        assertThat(messageDO.getPayload()).isEqualTo("");
    }
}
