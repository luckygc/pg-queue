package github.luckygc.pgq.config;

import github.luckygc.pgq.api.MessageHandler;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.springframework.util.StringUtils;

/**
 * 队列配置
 */
public class QueueConfig {

    /**
     * 主题
     */
    private final String topic;
    /**
     * 最大尝试次数,默认1
     */
    private final int maxAttempt;
    /**
     * 首次处理延迟,默认无延迟
     */
    private final Duration firstProcessDelay;
    /**
     * 下一次处理延迟,默认十分钟
     */
    private final Duration nextProcessDelay;
    /**
     * 消息处理器
     */
    private final MessageHandler messageHandler;
    /**
     * 处理器数量
     */
    private final int handlerCount;

    /**
     * 保留时间,默认一天
     */
    private final Duration retentionTime;

    private QueueConfig(Builder builder) {
        if (!StringUtils.hasText(builder.topic)) {
            throw new IllegalArgumentException("topic不能为空");
        }

        topic = builder.topic;

        if (builder.maxAttempt == null) {
            maxAttempt = 1;
        } else if (builder.maxAttempt < 1) {
            throw new IllegalArgumentException("maxAttempt不能小于1");
        } else {
            maxAttempt = builder.maxAttempt;
        }

        firstProcessDelay = builder.firstProcessDelay;

        if (builder.nexProcessDelay == null) {
            nextProcessDelay = Duration.ofMinutes(10);
        } else {
            nextProcessDelay = builder.nexProcessDelay;
        }

        if (builder.messageHandler == null) {
            throw new IllegalArgumentException("messageHandler不能为null");
        }

        messageHandler = builder.messageHandler;

        if (builder.handlerCount == null) {
            handlerCount = 1;
        } else if (builder.handlerCount < 1) {
            throw new IllegalArgumentException("handlerCount不能小于1");
        } else {
            handlerCount = builder.handlerCount;
        }

        if (builder.retentionTime == null) {
            retentionTime = Duration.ofDays(1);
        } else {
            retentionTime = builder.retentionTime;
        }
    }

    public String getTopic() {
        return topic;
    }

    public int getMaxAttempt() {
        return maxAttempt;
    }

    public Optional<Duration> getFirstProcessDelay() {
        return Optional.ofNullable(firstProcessDelay);
    }

    public Duration getNextProcessDelay() {
        return nextProcessDelay;
    }

    public MessageHandler getMessageHandler() {
        return messageHandler;
    }

    public int getHandlerCount() {
        return handlerCount;
    }

    public Duration getRetentionTime() {
        return retentionTime;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueueConfig that)) {
            return false;
        }
        return Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topic);
    }

    public static class Builder {

        private String topic;
        private Integer maxAttempt;
        private Duration firstProcessDelay;
        private Duration nexProcessDelay;
        private MessageHandler messageHandler;
        private Integer handlerCount;
        private Duration retentionTime;

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder maxAttempt(int maxAttempt) {
            this.maxAttempt = maxAttempt;
            return this;
        }

        public Builder firstProcessDelay(Duration firstProcessDelay) {
            this.firstProcessDelay = firstProcessDelay;
            return this;
        }

        public Builder nextProcessDelay(Duration nextProcessDelay) {
            this.nexProcessDelay = nextProcessDelay;
            return this;
        }

        public Builder messageHandler(MessageHandler messageHandler) {
            this.messageHandler = messageHandler;
            return this;
        }

        public Builder handlerCount(int handlerCount) {
            this.handlerCount = handlerCount;
            return this;
        }

        public Builder retentionTime(Duration retentionTime) {
            this.retentionTime = retentionTime;
            return this;
        }

        public QueueConfig build() {
            return new QueueConfig(this);
        }
    }
}
