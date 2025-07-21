package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageHandler;
import github.luckygc.pgq.api.MessageSerializable;
import github.luckygc.pgq.config.QueueConfig;
import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgQueueImpl<M> implements PgQueue<M> {

    private static final Logger log = LoggerFactory.getLogger(PgQueueImpl.class);

    private final QueueDao queueDao;
    private final QueueConfig config;
    private final MessageSerializable<M> messageSerializer;
    private final MessageHandler<M> messageHandler;
    private final Semaphore semaphore;

    public PgQueueImpl(QueueDao queueDao, QueueConfig config, MessageSerializable<M> messageSerializer,
            MessageHandler<M> messageHandler, Integer handlerCount) {
        this.config = Objects.requireNonNull(config);
        this.queueDao = Objects.requireNonNull(queueDao);
        this.messageSerializer = Objects.requireNonNull(messageSerializer);
        this.messageHandler = Objects.requireNonNull(messageHandler);
        if (handlerCount == null) {
            handlerCount = 1;
        }

        if (handlerCount < 1) {
            throw new IllegalArgumentException("handlerCount必须大于0");
        }
        semaphore = new Semaphore(handlerCount);
    }

    @Override
    public QueueConfig getConfig() {
        return config;
    }

    @Override
    public void push(M message) {
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setTopic(config.getTopic());
        LocalDateTime now = LocalDateTime.now();
        messageEntity.setCreateTime(now);
        messageEntity.setPayload(messageSerializer.serialize(message));
        messageEntity.setMaxAttempt(config.getMaxAttempt());

        messageEntity.setStatus(MessageStatus.PENDING);
        messageEntity.setAttempt(0);
        messageEntity.setNextProcessTime(now.plus(config.getFirstProcessDelay()));

        queueDao.insertMessageEntity(messageEntity);

        tryStartPollingAsync();
    }

    private void tryStartPollingAsync() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        Runnable runnable = () -> {
            try {
                polling();
            } finally {
                semaphore.release();
            }
        };

        Thread thread = new Thread(runnable, "pgq-%s".formatted(config.getTopic()));
        thread.setDaemon(true);
        thread.start();
    }

    private void polling() {
        List<MessageEntity> messageEntities;
        while (!(messageEntities = pull()).isEmpty()) {
            for (MessageEntity messageEntity : messageEntities) {
                try {
                    String payload = messageEntity.getPayload();
                    M message = messageSerializer.deserialize(payload);
                    boolean isSuccess = messageHandler.handle(message);
                    if (isSuccess) {
                        messageEntity.setAttempt(messageEntity.getAttempt() + 1);
                        messageEntity.setStatus(MessageStatus.COMPLETED);
                    } else {
                        handleFailed(messageEntity);
                    }
                } catch (Throwable t) {
                    log.error("处理消息异常", t);
                    handleFailed(messageEntity);
                }

                queueDao.updateMessageEntity(messageEntity);
            }
        }
    }

    private List<MessageEntity> pull() {
        return queueDao.findWaitHandleMessageEntities(config.getTopic(), config.getPullBatchSize());
    }

    private void handleFailed(MessageEntity messageEntity) {
        messageEntity.setAttempt(messageEntity.getAttempt() + 1);
        if (messageEntity.getAttempt().equals(messageEntity.getMaxAttempt())) {
            messageEntity.setStatus(MessageStatus.DEAD);
        } else {
            messageEntity.setStatus(MessageStatus.PENDING);
            messageEntity.setNextProcessTime(LocalDateTime.now().plus(config.getNextProcessDelay()));
        }
    }
}
