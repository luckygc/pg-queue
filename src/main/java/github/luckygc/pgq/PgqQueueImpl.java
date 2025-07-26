package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageGather;
import github.luckygc.pgq.api.PgQueue;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
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
        queueDao.insertMessage(buildMessageObj(message, PgqConstants.DEFAULT_PRIORITY));
    }

    @Override
    public void push(@Nullable List<String> messages) {
        if (messages == null) {
            return;
        }

        queueDao.insertMessages(buildMessageObjs(messages, PgqConstants.DEFAULT_PRIORITY));
    }

    private Message buildMessageObj(String message, int priority) {
        Message messageObj = new Message();
        messageObj.setCreateTime(LocalDateTime.now());
        messageObj.setTopic(topic);
        messageObj.setPriority(priority);
        messageObj.setPayload(message);
        messageObj.setAttempt(0);
        return messageObj;
    }

    private List<Message> buildMessageObjs(List<String> messages, int priority) {
        List<Message> messagesObjs = new ArrayList<>(messages.size());
        for (String message : messages) {
            Message messageObj = new Message();
            messageObj.setCreateTime(LocalDateTime.now());
            messageObj.setTopic(topic);
            messageObj.setPriority(priority);
            messageObj.setPayload(message);
            messageObj.setAttempt(0);
            messagesObjs.add(messageObj);
        }

        return messagesObjs;
    }

    @Override
    public MessageGather message(@Nullable String message) {
        return new MessageGatherImpl(message);
    }

    @Override
    public MessageGather messages(@Nullable List<String> messages) {
        return new MessageGatherImpl(messages);
    }

    @Override
    public @Nullable Message pull() {
        return queueDao.pull(topic);
    }

    @Override
    public List<Message> pull(int pullCount) {
        return queueDao.pull(topic, pullCount);
    }

    @Override
    public void complete(Message message, boolean delete) {
        queueDao.completeMessage(message, delete);
    }

    @Override
    public void complete(List<Message> messages, boolean delete) {
        queueDao.completeMessage(messages, delete);
    }

    @Override
    public void retry(Message message, Duration processDelay) {

    }

    @Override
    public void retry(List<Message> messages, Duration processDelay) {

    }

    @Override
    public void dead(Message message) {
        queueDao.deadMessage(message);
    }

    @Override
    public void dead(List<Message> messages) {
        queueDao.deadMessage(messages);
    }

    public class MessageGatherImpl implements MessageGather {

        private List<String> messages = null;
        private Integer priority;
        private Duration processDelay;

        public MessageGatherImpl(String message) {
            if (message != null) {
                this.messages = List.of(message);
            }
        }

        public MessageGatherImpl(@Nullable List<String> messages) {
            this.messages = messages;
        }

        @Override
        public MessageGather priority(int priority) {
            this.priority = priority;
            return this;
        }

        @Override
        public MessageGather processDelay(@Nullable Duration delay) {
            this.processDelay = delay;
            return this;
        }

        @Override
        public void push() {
            if (messages == null || messages.isEmpty()) {
                return;
            }

            int finalPriority = this.priority == null ? PgqConstants.DEFAULT_PRIORITY : this.priority;

            if (messages.size() == 1) {
                queueDao.insertMessage(buildMessageObj(messages.get(0), finalPriority), processDelay);
                return;
            }

            queueDao.insertMessages(buildMessageObjs(messages, finalPriority), processDelay);
        }
    }
}
