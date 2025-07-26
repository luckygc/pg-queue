package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageGather;
import github.luckygc.pgq.api.PgQueue;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.springframework.util.StringUtils;

public class PgqQueueImpl implements PgQueue {

    private final String topic;
    private final QueueDao queueDao;

    public PgqQueueImpl(PgqBuilder pgqBuilder) {
        if (!StringUtils.hasText(pgqBuilder.topic)) {
            throw new IllegalArgumentException("topic不能为空");
        }

        this.topic = pgqBuilder.topic;
        this.queueDao = Objects.requireNonNull(pgqBuilder.queueDao);
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void push(@Nullable String message) {
        if (message == null) {
            return;
        }
        queueDao.insertMessage(buildMessageObj(message));
    }

    @Override
    public void push(@Nullable List<String> messages) {
        if (messages == null) {
            return;
        }

        queueDao.insertMessages(buildMessageObjs(messages));
    }

    private Message buildMessageObj(String message) {
        Message messageObj = new Message();
        messageObj.setCreateTime(LocalDateTime.now());
        messageObj.setTopic(topic);
        messageObj.setPriority(PgqConstants.DEFAULT_PRIORITY);
        messageObj.setPayload(message);
        messageObj.setAttempt(0);
        return messageObj;
    }

    private List<Message> buildMessageObjs(List<String> messages) {
        return messages.stream().map(this::buildMessageObj).toList();
    }

    @Override
    public MessageGather message(@Nullable String message) {
        return null;
    }

    @Override
    public MessageGather messages(@Nullable List<String> messages) {
        return null;
    }

    @Override
    public @Nullable Message pull() {
        return null;
    }

    @Override
    public List<Message> pull(int pullCount) {
        return List.of();
    }

    @Override
    public void delete(Message message) {

    }

    @Override
    public void delete(List<Message> messages) {

    }

    @Override
    public void complete(Message message) {

    }

    @Override
    public void complete(List<Message> messages) {

    }

    @Override
    public void retry(Message message, Duration processDelay) {

    }

    @Override
    public void retry(List<Message> messages, Duration processDelay) {

    }

    @Override
    public void dead(Message message) {

    }

    @Override
    public void dead(List<Message> messages) {

    }

    public static class MessageGatherImpl implements MessageGather {

        private final QueueDao queueDao;
        private final List<String> messages;
        private Integer priority;
        private Duration processDelay;

        public MessageGatherImpl(QueueDao queueDao, String message) {
            this.queueDao = queueDao;
            this.messages = List.of(message);
        }

        public MessageGatherImpl(QueueDao queueDao, List<String> messages) {
            this.queueDao = queueDao;
            this.messages = messages;
        }

        @Override
        public MessageGather priority(int priority) {
            this.priority = priority;
            return this;
        }

        @Override
        public MessageGather processDelay(Duration delay) {
            this.processDelay = delay;
            return this;
        }

        @Override
        public void push() {

        }
    }
}
