package github.luckygc.pgq.impl;

import github.luckygc.pgq.MessageAvailableCallbackDispatcher;
import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PriorityMessageQueue;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.dao.QueueManagerDao;
import github.luckygc.pgq.model.MessageDO;
import github.luckygc.pgq.model.MessageDO.Builder;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class MessageQueueImpl implements MessageQueue, DelayMessageQueue, PriorityMessageQueue {

    private final MessageAvailableCallbackDispatcher messageAvailableCallbackDispatcher;
    private final MessageDao messageDao;
    private final boolean enablePgNotify;
    private QueueManagerDao queueManagerDao;

    public MessageQueueImpl(MessageDao messageDao, String topic, MessageAvailableCallbackDispatcher messageAvailableCallbackDispatcher) {
        this.messageDao = Objects.requireNonNull(messageDao);
        this.messageAvailableCallbackDispatcher = Objects.requireNonNull(messageAvailableCallbackDispatcher);
        this.enablePgNotify = false;
    }

    public MessageQueueImpl(MessageDao messageDao, String topic, MessageAvailableCallbackDispatcher messageAvailableCallbackDispatcher,
            QueueManagerDao queueManagerDao) {
        this.messageDao = Objects.requireNonNull(messageDao);
        this.messageAvailableCallbackDispatcher = Objects.requireNonNull(messageAvailableCallbackDispatcher);
        this.enablePgNotify = true;
        this.queueManagerDao = Objects.requireNonNull(queueManagerDao);
    }

    @Override
    public void send(String topic, String message) {
        MessageDO messageDOObj = Builder.create()
                .topic(topic)
                .payload(message)
                .build();
        messageDao.insertIntoPending(messageDOObj);

        messageAvailableCallbackDispatcher.dispatchCallback(topic);
        if (enablePgNotify) {
            queueManagerDao.sendNotify(topic);
        }
    }

    @Override
    public void send(String topic, String message, Duration processDelay) {
        MessageDO messageDOObj = MessageDO.of(topic, message, PgmqConstants.MESSAGE_PRIORITY);
        messageDao.insertIntoInvisible(messageDOObj, processDelay);
    }

    @Override
    public void send(String message, int priority) {
        MessageDO messageDOObj = MessageDO.of(topic, message, priority);
        messageDao.insertIntoPending(messageDOObj);
        dispatchAndSendNotify();
    }

    @Override
    public void send(String message, Duration processDelay, int priority) {
        MessageDO messageDOObj = MessageDO.of(topic, message, priority);
        messageDao.insertIntoInvisible(messageDOObj, processDelay);
    }

    @Override
    public void send(List<String> messages) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, PgmqConstants.MESSAGE_PRIORITY);
        messageDao.insertIntoPending(messageDOObjs);
        dispatchAndSendNotify();
    }

    @Override
    public void send(List<String> messages, Duration processDelay) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, PgmqConstants.MESSAGE_PRIORITY);
        messageDao.insertIntoInvosible(messageDOObjs, processDelay);
    }

    @Override
    public void send(List<String> messages, int priority) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, priority);
        messageDao.insertIntoPending(messageDOObjs);
        dispatchAndSendNotify();
    }

    @Override
    public void send(List<String> messages, Duration processDelay, int priority) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, priority);
        messageDao.insertIntoInvosible(messageDOObjs, processDelay);
    }

    private void dispatchAndSendNotify() {
        messageAvailableCallbackDispatcher.dispatchCallback(topic);
        if (enablePgNotify) {
            queueManagerDao.sendNotify(topic);
        }
    }

    @Override
    public @Nullable MessageDO pull() {
        List<MessageDO> messageDOS = messageDao.getPendingMessagesAndMoveToProcessing(topic, 1, PgmqConstants.PROCESS_TIMEOUT);
        if (messageDOS.isEmpty()) {
            return null;
        }

        return messageDOS.get(0);
    }

    @Override
    public @Nullable MessageDO pull(Duration processTimeout) {
        List<MessageDO> messageDOS = messageDao.getPendingMessagesAndMoveToProcessing(topic, 1, processTimeout);
        if (messageDOS.isEmpty()) {
            return null;
        }

        return messageDOS.get(0);
    }

    @Override
    public List<MessageDO> pull(int pullCount) {
        return messageDao.getPendingMessagesAndMoveToProcessing(topic, pullCount, PgmqConstants.PROCESS_TIMEOUT);
    }

    @Override
    public List<MessageDO> pull(int pullCount, Duration processTimeout) {
        return messageDao.getPendingMessagesAndMoveToProcessing(topic, pullCount, processTimeout);
    }
}
