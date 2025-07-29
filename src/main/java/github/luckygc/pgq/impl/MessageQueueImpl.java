package github.luckygc.pgq.impl;

import github.luckygc.pgq.MessageProcessorDispatcher;
import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.Utils;
import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PriorityMessageQueue;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.dao.QueueDao;
import github.luckygc.pgq.model.MessageDO;
import github.luckygc.pgq.model.MessageDO.Builder;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class MessageQueueImpl implements MessageQueue, DelayMessageQueue, PriorityMessageQueue {

    private final MessageProcessorDispatcher messageProcessorDispatcher;
    private final MessageDao messageDao;
    private final boolean enablePgNotify;
    private QueueDao queueDao;

    public MessageQueueImpl(MessageDao messageDao, String topic,
            MessageProcessorDispatcher messageProcessorDispatcher) {
        this.messageDao = Objects.requireNonNull(messageDao);
        this.messageProcessorDispatcher = Objects.requireNonNull(messageProcessorDispatcher);
        this.enablePgNotify = false;
    }

    public MessageQueueImpl(MessageDao messageDao,
            MessageProcessorDispatcher messageProcessorDispatcher) {
        this.messageDao = Objects.requireNonNull(messageDao);
        this.messageProcessorDispatcher = Objects.requireNonNull(messageProcessorDispatcher);
        this.enablePgNotify = true;
        this.queueDao = Objects.requireNonNull(queueDao);
    }

    @Override
    public void send(String topic, String message) {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic(topic)
                .payload(message)
                .attempt(0)
                .build();
        messageDao.insertIntoPending(messageDO);
    }

    @Override
    public void send(String topic, String message, Duration processDelay) {
        MessageDO messageDO = Builder.create()
                .topic(topic)
                .payload(message)
                .attempt(0)
                .build();

        Utils.checkDurationIsPositive(processDelay);
        LocalDateTime visibleTime = LocalDateTime.now().plus(processDelay);

        messageDao.insertIntoInvisible(messageDO, visibleTime);
    }

    @Override
    public void send(String topic, String message, int priority) {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic(topic)
                .payload(message)
                .priority(priority)
                .attempt(0)
                .build();
        messageDao.insertIntoPending(messageDO);
    }

    @Override
    public void send(String topic, List<String> messages) {
        Utils.checkMessagesNotEmpty(messages);

        List<MessageDO> messageDOS = new ArrayList<>(messages.size());

        for (String message : messages) {
            messageDOS.add(Builder.create()
                    .topic(topic)
                    .payload(message)
                    .attempt(0)
                    .build());
        }

        messageDao.insertIntoPending(messageDOS);
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

    @Override
    public @Nullable MessageDO pull() {
        List<MessageDO> messageDOS = messageDao.getPendingMessagesAndMoveToProcessing(topic, 1,
                PgmqConstants.PROCESS_TIMEOUT);
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
