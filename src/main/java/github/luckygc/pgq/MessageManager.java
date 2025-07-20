package github.luckygc.pgq;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import github.luckygc.pgq.api.MessagePuller;
import github.luckygc.pgq.api.MessagePusher;
import github.luckygc.pgq.config.QueueConfig;
import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;

public class MessageManager implements MessagePusher, MessagePuller {

    private final QueueConfig queueConfig;
    private final QueueDao queueDao;
    private final ObjectMapper objectMapper;
    private final ThreadPoolExecutor threadPoolExecutor;

    public MessageManager(QueueConfig queueConfig, QueueDao queueDao, ObjectMapper objectMapper) {
        this.queueConfig = queueConfig;
        this.queueDao = queueDao;
        this.objectMapper = objectMapper;
        this.threadPoolExecutor = createThreadPoolExecutor();
    }

    private ThreadPoolExecutor createThreadPoolExecutor() {
        int handlerCount = queueConfig.getHandlerCount();
        String topic = queueConfig.getTopic();

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(@NonNull Runnable r) {
                Thread thread = new Thread(r, "pgq-" + topic + "-handler-" + threadNumber.getAndIncrement());
                thread.setDaemon(false);
                return thread;
            }
        };

        return new ThreadPoolExecutor(
                1,                           // corePoolSize：核心线程数
                handlerCount,                           // maximumPoolSize：最大线程数
                60L,                                    // keepAliveTime：空闲线程存活时间
                TimeUnit.SECONDS,                       // unit：时间单位
                new ArrayBlockingQueue<>(5000),         // workQueue：工作队列，容量1000
                threadFactory,                          // threadFactory：线程工厂
                new ThreadPoolExecutor.CallerRunsPolicy() // handler：拒绝策略
        );
    }

    @Override
    public void push(Object message) {
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

        return Optional.of(messageEntities.get(0));
    }

    @Override
    public List<MessageEntity> pull(long pullCount) {
        return queueDao.findWaitHandleMessageEntities(queueConfig.getTopic(), pullCount);
    }
}
