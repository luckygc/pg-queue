package github.luckygc.pgq.model;

import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.Utils;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class Message {

    private Message(Builder builder) {
        this.id = builder.id;
        this.createTime = Objects.requireNonNullElseGet(builder.createTime, LocalDateTime::now);
        this.priority = Objects.requireNonNullElse(builder.priority, PgmqConstants.MESSAGE_PRIORITY);
        this.topic = Objects.requireNonNull(builder.topic);
        this.payload = Objects.requireNonNull(builder.payload);
        this.attempt = Objects.requireNonNullElse(builder.attempt, 0);
    }

    @Nullable
    private final Long id;

    private final LocalDateTime createTime;

    private final String payload;

    private final String topic;

    private final Integer priority;

    private final Integer attempt;

    public static Message of(String topic, String message, int priority) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(message);

        return new Builder()
                .createTime(LocalDateTime.now())
                .topic(topic)
                .priority(priority)
                .payload(message)
                .attempt(0)
                .build();
    }

    public static List<Message> of(String topic, List<String> messages, int priority) {
        Objects.requireNonNull(topic);
        Utils.checkMessagesNotEmpty(messages);

        List<Message> messagesObjs = new ArrayList<>(messages.size());
        LocalDateTime now = LocalDateTime.now();
        for (String message : messages) {
            Message messageObj = new Builder()
                    .createTime(now)
                    .topic(topic)
                    .priority(priority)
                    .payload(message)
                    .attempt(0)
                    .build();
            messagesObjs.add(messageObj);
        }

        return messagesObjs;
    }

    @Nullable
    public Long getId() {
        return id;
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

        private Long id;
        private LocalDateTime createTime;
        private String payload;
        private String topic;
        private Integer priority;
        private Integer attempt;

        public Builder id(Long id) {
            this.id = id;
            return this;
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

        public Message build() {
            return new Message(this);
        }
    }
}
