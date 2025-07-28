package github.luckygc.pgq.impl;

import github.luckygc.pgq.ListenerDispatcher;
import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PriorityMessageQueue;
import github.luckygc.pgq.dao.DatabaseQueueDao;
import github.luckygc.pgq.dao.QueueManagerDao;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class MessageQueueImpl implements MessageQueue, DelayMessageQueue, PriorityMessageQueue {

    private final ListenerDispatcher listenerDispatcher;
    private final DatabaseQueueDao databaseQueueDao;
    private final boolean enablePgNotify;
    private QueueManagerDao queueManagerDao;

    public MessageQueueImpl(DatabaseQueueDao databaseQueueDao, String topic, ListenerDispatcher listenerDispatcher) {
        this.databaseQueueDao = Objects.requireNonNull(databaseQueueDao);
        this.listenerDispatcher = Objects.requireNonNull(listenerDispatcher);
        this.enablePgNotify = false;
    }

    public MessageQueueImpl(DatabaseQueueDao databaseQueueDao, String topic, ListenerDispatcher listenerDispatcher,
            QueueManagerDao queueManagerDao) {
        this.databaseQueueDao = Objects.requireNonNull(databaseQueueDao);
        this.listenerDispatcher = Objects.requireNonNull(listenerDispatcher);
        this.enablePgNotify = true;
        this.queueManagerDao = Objects.requireNonNull(queueManagerDao);
    }

    @Override
    public void send(String topic, String message) {
        Message messageObj = Message.of(topic, message, PgmqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertMessage(messageObj);
        dispatchAndSendNotify();
    }

    @Override
    public void send(String topic, String message, Duration processDelay) {
        Message messageObj = Message.of(topic, message, PgmqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void send(String message, int priority) {
        Message messageObj = Message.of(topic, message, priority);
        databaseQueueDao.insertMessage(messageObj);
        dispatchAndSendNotify();
    }

    @Override
    public void send(String message, Duration processDelay, int priority) {
        Message messageObj = Message.of(topic, message, priority);
        databaseQueueDao.insertProcessLaterMessage(messageObj, processDelay);
    }

    @Override
    public void send(List<String> messages) {
        List<Message> messageObjs = Message.of(topic, messages, PgmqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertMessages(messageObjs);
        dispatchAndSendNotify();
    }

    @Override
    public void send(List<String> messages, Duration processDelay) {
        List<Message> messageObjs = Message.of(topic, messages, PgmqConstants.MESSAGE_PRIORITY);
        databaseQueueDao.insertProcessLaterMessages(messageObjs, processDelay);
    }

    @Override
    public void send(List<String> messages, int priority) {
        List<Message> messageObjs = Message.of(topic, messages, priority);
        databaseQueueDao.insertMessages(messageObjs);
        dispatchAndSendNotify();
    }

    @Override
    public void send(List<String> messages, Duration processDelay, int priority) {
        List<Message> messageObjs = Message.of(topic, messages, priority);
        databaseQueueDao.insertProcessLaterMessages(messageObjs, processDelay);
    }

    private void dispatchAndSendNotify() {
        listenerDispatcher.dispatchWithCheckTx(topic);
        if (enablePgNotify) {
            queueManagerDao.sendNotify(topic);
        }
    }

    @Override
    public @Nullable Message pull() {
        List<Message> messages = databaseQueueDao.pull(topic, 1, PgmqConstants.PROCESS_TIMEOUT);
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
        return databaseQueueDao.pull(topic, pullCount, PgmqConstants.PROCESS_TIMEOUT);
    }

    @Override
    public List<Message> pull(int pullCount, Duration processTimeout) {
        return databaseQueueDao.pull(topic, pullCount, processTimeout);
    }
}
