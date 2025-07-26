package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageListener;
import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.ProcessingMessageManager;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class PgQueueImpl implements PgQueue {

    private final QueueDao queueDao;
    private final String topic;
    private final ProcessingMessageManager processingMessageManager;
    @Nullable
    private final MessageListener messageListener;

    public PgQueueImpl(QueueDao queueDao, String topic, ProcessingMessageManager processingMessageManager,
            @Nullable MessageListener messageListener) {
        this.queueDao = Objects.requireNonNull(queueDao);
        this.topic = Objects.requireNonNull(topic);
        this.processingMessageManager = Objects.requireNonNull(processingMessageManager);
        this.messageListener = messageListener;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public ProcessingMessageManager processingMessageManager() {
        return processingMessageManager;
    }

    @Nullable
    @Override
    public MessageListener messageListener() {
        return messageListener;
    }

    @Override
    public void push(String message) {
        Message messageObj = Utils.buildMessageObj(topic, message, PgqConstants.MESSAGE_PRIORITY);
        queueDao.insertMessage(messageObj);
    }

    @Override
    public void push(String message, Duration processDelay) {
        Message messageObj = Utils.buildMessageObj(topic, message, PgqConstants.MESSAGE_PRIORITY);
        queueDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void push(String message, int priority) {
        Message messageObj = Utils.buildMessageObj(topic, message, priority);
        queueDao.insertMessage(messageObj);
    }

    @Override
    public void push(String message, Duration processDelay, int priority) {
        Message messageObj = Utils.buildMessageObj(topic, message, priority);
        queueDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void push(List<String> messages) {
        List<Message> messageObjs = Utils.buildMessageObjs(topic, messages, PgqConstants.MESSAGE_PRIORITY);
        queueDao.insertMessages(messageObjs);
    }

    @Override
    public void push(List<String> messages, Duration processDelay) {
        List<Message> messageObjs = Utils.buildMessageObjs(topic, messages, PgqConstants.MESSAGE_PRIORITY);
        queueDao.insertProcessLaterMessages(messageObjs, processDelay);
    }

    @Override
    public void push(List<String> messages, int priority) {
        List<Message> messageObjs = Utils.buildMessageObjs(topic, messages, priority);
        queueDao.insertMessages(messageObjs);
    }

    @Override
    public void push(List<String> messages, Duration processDelay, int priority) {
        List<Message> messageObjs = Utils.buildMessageObjs(topic, messages, priority);
        queueDao.insertProcessLaterMessages(messageObjs, processDelay);
    }

    @Override
    public @Nullable Message pull() {
        List<Message> messages = queueDao.pull(topic, 1, PgqConstants.PROCESS_TIMEOUT);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    @Override
    public @Nullable Message pull(Duration processTimeout) {
        List<Message> messages = queueDao.pull(topic, 1, processTimeout);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    @Override
    public List<Message> pull(int pullCount) {
        return queueDao.pull(topic, pullCount, PgqConstants.PROCESS_TIMEOUT);
    }

    @Override
    public List<Message> pull(int pullCount, Duration processTimeout) {
        return queueDao.pull(topic, pullCount, processTimeout);
    }
}
