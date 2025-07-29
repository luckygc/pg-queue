package github.luckygc.pgq.impl;

import github.luckygc.pgq.tool.MessageProcessorDispatcher;
import github.luckygc.pgq.tool.PgNotifier;
import github.luckygc.pgq.model.PgmqConstants;
import github.luckygc.pgq.tool.Checker;
import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PriorityMessageQueue;
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
    private final MessageProcessorDispatcher dispatcher;
    private final @Nullable PgNotifier pgNotifier;

    public MessageQueueImpl(MessageDao messageDao, MessageProcessorDispatcher dispatcher,
            @Nullable PgNotifier pgNotifier) {
        this.messageDao = Objects.requireNonNull(messageDao);
        this.dispatcher = Objects.requireNonNull(dispatcher);
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

        dispatcher.dispatch(topic);
    }

    @Override
    public void send(String topic, String message, Duration processDelay) {
        MessageDO messageDO = Builder.create()
                .topic(topic)
                .priority(PgmqConstants.MESSAGE_PRIORITY)
                .payload(message)
                .attempt(0)
                .build();

        Checker.checkDurationIsPositive(processDelay);
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

        dispatcher.dispatch(topic);
    }

    @Override
    public void send(String topic, List<String> messages) {
        Checker.checkMessagesNotEmpty(messages);

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

        dispatcher.dispatch(topic);
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

        Checker.checkDurationIsPositive(processDelay);
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

        dispatcher.dispatch(topic);
    }

    @Override
    public @Nullable Message poll(String topic) {
        LocalDateTime processTimeoutTime = LocalDateTime.now().plus(PgmqConstants.PROCESS_TIMEOUT);
        List<Message> messages = messageDao.getPendingMessagesAndMoveToProcessing(topic, 1, processTimeoutTime);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    @Override
    public List<Message> poll(String topic, int maxPoll) {
        Checker.checkMaxPollRange(maxPoll);
        LocalDateTime processTimeoutTime = LocalDateTime.now().plus(PgmqConstants.PROCESS_TIMEOUT);
        return messageDao.getPendingMessagesAndMoveToProcessing(topic, maxPoll, processTimeoutTime);
    }
}
