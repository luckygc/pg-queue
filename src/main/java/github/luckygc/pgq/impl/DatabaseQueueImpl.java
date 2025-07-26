package github.luckygc.pgq.impl;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.PgqConstants;
import github.luckygc.pgq.QueueDao;
import github.luckygc.pgq.Utils;
import github.luckygc.pgq.api.DatabaseQueue;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class DatabaseQueueImpl implements DatabaseQueue {

    private final QueueDao queueDao;
    private final String topic;

    public DatabaseQueueImpl(QueueDao queueDao, String topic) {
        this.queueDao = Objects.requireNonNull(queueDao);
        this.topic = Objects.requireNonNull(topic);
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
