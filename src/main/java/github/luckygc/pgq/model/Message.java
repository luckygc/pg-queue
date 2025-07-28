package github.luckygc.pgq.model;

import github.luckygc.pgq.Utils;
import github.luckygc.pgq.dao.MessageDao;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

public class Message {

    private final Long id;

    private final LocalDateTime createTime;

    private final String payload;

    private final String topic;

    private final Integer priority;

    private final Integer attempt;

    private final MessageDao messageDao;

    private Message(Builder builder) {
        this.id = Objects.requireNonNull(builder.id);
        this.createTime = Objects.requireNonNull(builder.createTime);
        this.priority = Objects.requireNonNull(builder.priority);
        this.topic = Objects.requireNonNull(builder.topic);
        this.payload = Objects.requireNonNull(builder.payload);
        this.attempt = Objects.requireNonNull(builder.attempt);
        this.messageDao = Objects.requireNonNull(builder.messageDao);
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

    public void delete() {
        messageDao.deleteProcessingMsgById(id);
    }

    public void dead() {
        messageDao.moveProcessingMsgToDeadById(id);
    }

    public void retry() {
        messageDao.moveProcessingMsgToPendingById(id);
    }

    public void retry(Duration processDelay) {
        Objects.requireNonNull(processDelay);
        Utils.checkDurationIsPositive(processDelay);

        LocalDateTime visibleTime = LocalDateTime.now().plus(processDelay);
        messageDao.moveProcessingMsgToInvisibleById(id, visibleTime);
    }

    public static class Builder {

        private Long id;
        private LocalDateTime createTime;
        private String payload;
        private String topic;
        private Integer priority;
        private Integer attempt;
        private MessageDao messageDao;

        public static Builder create() {
            return new Builder();
        }

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


        public Builder messageDao(MessageDao messageDao) {
            this.messageDao = messageDao;
            return this;
        }

        public Message build() {
            return new Message(this);
        }
    }
}
