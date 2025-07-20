package github.luckygc.pgq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import github.luckygc.pgq.api.MessageProducer;
import github.luckygc.pgq.api.MessagePuller;
import github.luckygc.pgq.config.QueueConfig;
import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public class MessageManager implements MessageProducer, MessagePuller {

    private final QueueConfig queueConfig;
    private final QueueDao queueDao;
    private final ObjectMapper objectMapper;

    public MessageManager(QueueConfig queueConfig, QueueDao queueDao, ObjectMapper objectMapper) {
        this.queueConfig = queueConfig;
        this.queueDao = queueDao;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(Object message) {
        MessageEntity messageEntity = new MessageEntity();
        LocalDateTime now = LocalDateTime.now();
        messageEntity.setCreateTime(now);
        try {
            messageEntity.setPayload(objectMapper.writeValueAsString(message));
        } catch (JsonProcessingException e) {
            throw new PublishException("message序列化失败", e);
        }

        messageEntity.setAttempt(0);
        messageEntity.setMaxAttempt(queueConfig.getMaxAttempt());

        queueConfig.getFirstProcessDelay()
                .ifPresentOrElse(delay -> messageEntity.setNextProcessTime(now.plus(delay)),
                        () -> messageEntity.setNextProcessTime(now));

        messageEntity.setTopic(queueConfig.getTopic());
        messageEntity.setStatus(MessageStatus.PENDING);

        queueDao.insertMessageEntity(messageEntity);
    }

    @Override
    public Optional<MessageEntity> pull() {
        List<MessageEntity> messageEntities = queueDao.findWaitHandleMessageEntities(queueConfig.getTopic(),
                1);

        if (messageEntities.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(messageEntities.getFirst());
    }

    @Override
    public List<MessageEntity> pull(long pullCount) {
        return queueDao.findWaitHandleMessageEntities(queueConfig.getTopic(), pullCount);
    }
}
