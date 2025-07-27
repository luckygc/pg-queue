package github.luckygc.pgq.impl;

import github.luckygc.pgq.ListenerDispatcher;
import github.luckygc.pgq.PgqConstants;
import github.luckygc.pgq.dao.DatabaseQueueDao;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class DatabaseQueueImpl implements DatabaseQueue {

    private final ListenerDispatcher listenerDispatcher;
    private final DatabaseQueueDao databaseQueueDao;
    private final String topic;

    public DatabaseQueueImpl(DatabaseQueueDao databaseQueueDao, String topic, ListenerDispatcher listenerDispatcher) {
        this.databaseQueueDao = Objects.requireNonNull(databaseQueueDao);
        this.topic = Objects.requireNonNull(topic);
        this.listenerDispatcher = Objects.requireNonNull(listenerDispatcher);
    }

    @Override
    public void push(String message) {
        Message messageObj = Message.of(topic, message, PgqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertMessage(messageObj);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(String message, Duration processDelay) {
        Message messageObj = Message.of(topic, message, PgqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void push(String message, int priority) {
        Message messageObj = Message.of(topic, message, priority);
        databaseQueueDao.insertMessage(messageObj);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(String message, Duration processDelay, int priority) {
        Message messageObj = Message.of(topic, message, priority);
        databaseQueueDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void push(List<String> messages) {
        List<Message> messageObjs = Message.of(topic, messages, PgqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertMessages(messageObjs);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(List<String> messages, Duration processDelay) {
        List<Message> messageObjs = Message.of(topic, messages, PgqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertProcessLaterMessages(messageObjs, processDelay);
    }

    @Override
    public void push(List<String> messages, int priority) {
        List<Message> messageObjs = Message.of(topic, messages, priority);
        databaseQueueDao.insertMessages(messageObjs);
        listenerDispatcher.dispatchWithCheckTx(topic);
    }

    @Override
    public void push(List<String> messages, Duration processDelay, int priority) {
        List<Message> messageObjs = Message.of(topic, messages, priority);
        databaseQueueDao.insertProcessLaterMessages(messageObjs, processDelay);
    }

    @Override
    public @Nullable Message pull() {
        List<Message> messages = databaseQueueDao.pull(topic, 1, PgqConstants.PROCESS_TIMEOUT);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    @Override
    public @Nullable Message pull(Duration processTimeout) {
        List<Message> messages = databaseQueueDao.pull(topic, 1, processTimeout);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    @Override
    public List<Message> pull(int pullCount) {
        return databaseQueueDao.pull(topic, pullCount, PgqConstants.PROCESS_TIMEOUT);
    }

    @Override
    public List<Message> pull(int pullCount, Duration processTimeout) {
        return databaseQueueDao.pull(topic, pullCount, processTimeout);
    }
}
