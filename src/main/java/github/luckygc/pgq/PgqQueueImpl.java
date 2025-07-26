package github.luckygc.pgq;

import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.ProcessingMessageManager;
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
    public ProcessingMessageManager processingMessageManager() {
        return null;
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
    public void push(String message, @Nullable Duration processDelay) {

    }

    @Override
    public void push(String message, int priority) {

    }

    @Override
    public void push(String message, @Nullable Duration processDelay, int priority) {

    }

    @Override
    public void push(@Nullable List<String> messages) {
        if (messages == null) {
            return;
        }

        queueDao.insertMessages(buildMessageObjs(messages, PgqConstants.DEFAULT_PRIORITY));
    }

    @Override
    public void push(List<String> messages, @Nullable Duration processDelay) {

    }

    @Override
    public void push(List<String> messages, int priority) {

    }

    @Override
    public void push(List<String> messages, @Nullable Duration processDelay, int priority) {

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
    public @Nullable Message pull() {
        return queueDao.pull(topic);
    }

    @Override
    public @Nullable Message pull(Duration processTimeout) {
        return null;
    }

    @Override
    public List<Message> pull(int pullCount) {
        return queueDao.pull(topic, pullCount);
    }

    @Override
    public List<Message> pull(int pullCount, Duration processTimeout) {
        return List.of();
    }

}
