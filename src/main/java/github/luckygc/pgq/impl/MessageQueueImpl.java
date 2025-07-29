package github.luckygc.pgq.impl;

import github.luckygc.pgq.PgNotifier;
import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.Utils;
import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PriorityMessageQueue;
import github.luckygc.pgq.api.callback.MessageAvailableCallback;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.model.MessageDO;
import github.luckygc.pgq.model.MessageDO.Builder;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class MessageQueueImpl implements MessageQueue, DelayMessageQueue, PriorityMessageQueue {


    private final MessageDao messageDao;
    private final MessageAvailableCallback callback;
    @Nullable
    private final PgNotifier pgNotifier;

    public MessageQueueImpl(MessageDao messageDao, MessageAvailableCallback callback, PgNotifier pgNotifier) {
        this.messageDao = Objects.requireNonNull(messageDao);
        this.callback = Objects.requireNonNull(callback);
        this.pgNotifier = pgNotifier;
    }

    @Override
    public void send(String topic, String message) {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic(topic)
                .priority(PgmqConstants.MESSAGE_PRIORITY)
                .payload(message)
                .attempt(0)
                .build();
        messageDao.insertIntoPending(messageDO);

        if (pgNotifier != null) {
            pgNotifier.sendNotify(topic);
        }

        callback.onMessageAvailable(topic);
    }

    @Override
    public void send(String topic, String message, Duration processDelay) {
        MessageDO messageDO = Builder.create()
                .topic(topic)
                .priority(PgmqConstants.MESSAGE_PRIORITY)
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
                .priority(priority)
                .payload(message)
                .attempt(0)
                .build();
        messageDao.insertIntoPending(messageDO);

        if (pgNotifier != null) {
            pgNotifier.sendNotify(topic);
        }

        callback.onMessageAvailable(topic);
    }

    @Override
    public void send(String topic, List<String> messages) {
        Utils.checkMessagesNotEmpty(messages);

        List<MessageDO> messageDOS = new ArrayList<>(messages.size());

        for (String message : messages) {
            messageDOS.add(Builder.create()
                    .topic(topic)
                    .priority(PgmqConstants.MESSAGE_PRIORITY)
                    .payload(message)
                    .attempt(0)
                    .build());
        }

        messageDao.insertIntoPending(messageDOS);

        if (pgNotifier != null) {
            pgNotifier.sendNotify(topic);
        }

        callback.onMessageAvailable(topic);
    }

    @Override
    public void send(String topic, List<String> messages, Duration processDelay) {
        List<MessageDO> messageDOS = new ArrayList<>(messages.size());

        for (String message : messages) {
            messageDOS.add(Builder.create()
                    .topic(topic)
                    .priority(PgmqConstants.MESSAGE_PRIORITY)
                    .payload(message)
                    .attempt(0)
                    .build());
        }

        Utils.checkDurationIsPositive(processDelay);
        LocalDateTime visibleTime = LocalDateTime.now().plus(processDelay);
        messageDao.insertIntoInvisible(messageDOS, visibleTime);
    }

    @Override
    public void send(String topic, List<String> messages, int priority) {
        List<MessageDO> messageDOS = new ArrayList<>(messages.size());

        for (String message : messages) {
            messageDOS.add(Builder.create()
                    .topic(topic)
                    .priority(priority)
                    .payload(message)
                    .attempt(0)
                    .build());
        }

        messageDao.insertIntoPending(messageDOS);

        if (pgNotifier != null) {
            pgNotifier.sendNotify(topic);
        }

        callback.onMessageAvailable(topic);
    }

    @Override
    public @Nullable Message poll(String topic) {
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
