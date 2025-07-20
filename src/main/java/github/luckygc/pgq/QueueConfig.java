package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageHandler;
import java.time.Duration;
import java.util.Optional;
import org.springframework.util.StringUtils;

public class QueueConfig {

    private final String topic;
    private final int maxAttempt;
    private final Duration firstProcessDelay;
    private final Duration nextProcessDelay;
    private final MessageHandler messageHandler;
    private final int handlerCount;

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

    public static class Builder {

        private String topic;
        private Integer maxAttempt;
        private Duration firstProcessDelay;
        private Duration nexProcessDelay;
        private MessageHandler messageHandler;
        private Integer handlerCount;
    }
}
