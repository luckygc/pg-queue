package github.luckygc.pgq.impl;

import github.luckygc.pgq.ListenerDispatcher;
import github.luckygc.pgq.PgqConstants;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class DatabaseQueueImpl implements DatabaseQueue {

    private final ListenerDispatcher listenerDispatcher;
    private final MessageDao messageDao;
    private final String topic;

    public DatabaseQueueImpl(MessageDao messageDao, String topic, ListenerDispatcher listenerDispatcher) {
        this.messageDao = Objects.requireNonNull(messageDao);
        this.topic = Objects.requireNonNull(topic);
        this.listenerDispatcher = Objects.requireNonNull(listenerDispatcher);
    }

    @Override
    public void push(String message) {
        Message messageObj = Message.of(topic, message, PgqConstants.MESSAGE_PRIORITY);
        messageDao.insertMessage(messageObj);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(String message, Duration processDelay) {
        Message messageObj = Message.of(topic, message, PgqConstants.MESSAGE_PRIORITY);
        messageDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void push(String message, int priority) {
        Message messageObj = Message.of(topic, message, priority);
        messageDao.insertMessage(messageObj);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(String message, Duration processDelay, int priority) {
        Message messageObj = Message.of(topic, message, priority);
        messageDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void push(List<String> messages) {
        List<Message> messageObjs = Message.of(topic, messages, PgqConstants.MESSAGE_PRIORITY);
        messageDao.insertMessages(messageObjs);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(List<String> messages, Duration processDelay) {
        List<Message> messageObjs = Message.of(topic, messages, PgqConstants.MESSAGE_PRIORITY);
        messageDao.insertProcessLaterMessages(messageObjs, processDelay);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(List<String> messages, int priority) {
        List<Message> messageObjs = Message.of(topic, messages, priority);
        messageDao.insertMessages(messageObjs);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(List<String> messages, Duration processDelay, int priority) {
        List<Message> messageObjs = Message.of(topic, messages, priority);
        messageDao.insertProcessLaterMessages(messageObjs, processDelay);
    }

    @Override
    public @Nullable Message pull() {
        List<Message> messages = messageDao.pull(topic, 1, PgqConstants.PROCESS_TIMEOUT);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    @Override
    public @Nullable Message pull(Duration processTimeout) {
        List<Message> messages = messageDao.pull(topic, 1, processTimeout);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    @Override
    public List<Message> pull(int pullCount) {
        return messageDao.pull(topic, pullCount, PgqConstants.PROCESS_TIMEOUT);
    }

    @Override
    public List<Message> pull(int pullCount, Duration processTimeout) {
        return messageDao.pull(topic, pullCount, processTimeout);
    }
}
