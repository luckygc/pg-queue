package github.luckygc.pgq.model;

import github.luckygc.pgq.PgmqConstants;
import java.time.LocalDateTime;
import java.util.Objects;

public class MessageDO {

    private final LocalDateTime createTime;

    private final String payload;

    private final String topic;

    private final Integer priority;

    private final Integer attempt;

    private MessageDO(Builder builder) {
        this.createTime = Objects.requireNonNullElseGet(builder.createTime, LocalDateTime::now);
        this.priority = Objects.requireNonNullElse(builder.priority, PgmqConstants.MESSAGE_PRIORITY);
        this.topic = Objects.requireNonNull(builder.topic);
        this.payload = Objects.requireNonNull(builder.payload);
        this.attempt = Objects.requireNonNull(builder.attempt);
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public String getPayload() {
        return payload;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPriority() {
        return priority;
    }

    public Integer getAttempt() {
        return attempt;
    }

    public static class Builder {

        private LocalDateTime createTime;
        private String payload;
        private String topic;
        private Integer priority;
        private Integer attempt;

        public static Builder create() {
            return new Builder();
        }

        public Builder createTime(LocalDateTime createTime) {
            this.createTime = createTime;
            return this;
        }

        public Builder payload(String payload) {
            this.payload = payload;
            return this;
        }


        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder priority(Integer priority) {
            this.priority = priority;
            return this;
        }

        public Builder attempt(Integer attempt) {
            this.attempt = attempt;
            return this;
        }

        public MessageDO build() {
            return new MessageDO(this);
        }
    }
}
