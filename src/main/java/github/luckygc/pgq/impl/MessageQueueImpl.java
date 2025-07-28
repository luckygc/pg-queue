package github.luckygc.pgq.impl;

import github.luckygc.pgq.MessageAvailableCallbackDispatcher;
import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PriorityMessageQueue;
import github.luckygc.pgq.dao.MessageQueueDao;
import github.luckygc.pgq.dao.QueueManagerDao;
import github.luckygc.pgq.model.MessageDO;
import github.luckygc.pgq.model.MessageDO.Builder;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class MessageQueueImpl implements MessageQueue, DelayMessageQueue, PriorityMessageQueue {

    private final MessageAvailableCallbackDispatcher messageAvailableCallbackDispatcher;
    private final MessageQueueDao messageQueueDao;
    private final boolean enablePgNotify;
    private QueueManagerDao queueManagerDao;

    public MessageQueueImpl(MessageQueueDao messageQueueDao, String topic, MessageAvailableCallbackDispatcher messageAvailableCallbackDispatcher) {
        this.messageQueueDao = Objects.requireNonNull(messageQueueDao);
        this.messageAvailableCallbackDispatcher = Objects.requireNonNull(messageAvailableCallbackDispatcher);
        this.enablePgNotify = false;
    }

    public MessageQueueImpl(MessageQueueDao messageQueueDao, String topic, MessageAvailableCallbackDispatcher messageAvailableCallbackDispatcher,
            QueueManagerDao queueManagerDao) {
        this.messageQueueDao = Objects.requireNonNull(messageQueueDao);
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
        messageQueueDao.insertMessage(messageDOObj);

        messageAvailableCallbackDispatcher.dispatchCallback(topic);
        if (enablePgNotify) {
            queueManagerDao.sendNotify(topic);
        }
    }

    @Override
    public void send(String topic, String message, Duration processDelay) {
        MessageDO messageDOObj = MessageDO.of(topic, message, PgmqConstants.MESSAGE_PRIORITY);
        messageQueueDao.insertProcessLaterMessage(messageDOObj, processDelay);
    }

    @Override
    public void send(String message, int priority) {
        MessageDO messageDOObj = MessageDO.of(topic, message, priority);
        messageQueueDao.insertMessage(messageDOObj);
        dispatchAndSendNotify();
    }

    @Override
    public void send(String message, Duration processDelay, int priority) {
        MessageDO messageDOObj = MessageDO.of(topic, message, priority);
        messageQueueDao.insertProcessLaterMessage(messageDOObj, processDelay);
    }

    @Override
    public void send(List<String> messages) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, PgmqConstants.MESSAGE_PRIORITY);
        messageQueueDao.insertMessages(messageDOObjs);
        dispatchAndSendNotify();
    }

    @Override
    public void send(List<String> messages, Duration processDelay) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, PgmqConstants.MESSAGE_PRIORITY);
        messageQueueDao.insertProcessLaterMessages(messageDOObjs, processDelay);
    }

    @Override
    public void send(List<String> messages, int priority) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, priority);
        messageQueueDao.insertMessages(messageDOObjs);
        dispatchAndSendNotify();
    }

    @Override
    public void send(List<String> messages, Duration processDelay, int priority) {
        List<MessageDO> messageDOObjs = MessageDO.of(topic, messages, priority);
        messageQueueDao.insertProcessLaterMessages(messageDOObjs, processDelay);
    }

    private void dispatchAndSendNotify() {
        messageAvailableCallbackDispatcher.dispatchCallback(topic);
        if (enablePgNotify) {
            queueManagerDao.sendNotify(topic);
        }
    }

    @Override
    public @Nullable MessageDO pull() {
        List<MessageDO> messageDOS = messageQueueDao.getPendingMessagesAndMoveToProcessing(topic, 1, PgmqConstants.PROCESS_TIMEOUT);
        if (messageDOS.isEmpty()) {
            return null;
        }

        return messageDOS.get(0);
    }

    @Override
    public @Nullable MessageDO pull(Duration processTimeout) {
        List<MessageDO> messageDOS = messageQueueDao.getPendingMessagesAndMoveToProcessing(topic, 1, processTimeout);
        if (messageDOS.isEmpty()) {
            return null;
        }

        return messageDOS.get(0);
    }

    @Override
    public List<MessageDO> pull(int pullCount) {
        return messageQueueDao.getPendingMessagesAndMoveToProcessing(topic, pullCount, PgmqConstants.PROCESS_TIMEOUT);
    }

    @Override
    public List<MessageDO> pull(int pullCount, Duration processTimeout) {
        return messageQueueDao.getPendingMessagesAndMoveToProcessing(topic, pullCount, processTimeout);
    }
}
