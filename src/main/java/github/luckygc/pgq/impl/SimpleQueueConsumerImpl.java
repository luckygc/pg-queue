package github.luckygc.pgq.impl;

import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.api.QueueConsumer;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.annotation.Transactional;

/**
 * 简单队列消费者实现.
 */
public class SimpleQueueConsumerImpl implements QueueConsumer {

    private static final String PULL_MESSAGE_SQL = """
            SELECT id, create_time, payload, topic, status, 
                   next_process_time, attempt, max_attempt
            FROM pgq_simple_queue 
            WHERE topic = :topic 
              AND status = 'PENDING' 
              AND next_process_time <= :currentTime
            ORDER BY next_process_time ASC, id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            """;

    private static final String PULL_MESSAGES_SQL = """
            SELECT id, create_time, payload, topic, status, 
                   next_process_time, attempt, max_attempt
            FROM pgq_simple_queue 
            WHERE topic = :topic 
              AND status = 'PENDING' 
              AND next_process_time <= :currentTime
            ORDER BY next_process_time ASC, id ASC
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """;

    private static final String UPDATE_STATUS_TO_PROCESSING_SQL = """
            UPDATE pgq_simple_queue 
            SET status = 'PROCESSING',
                attempt = attempt + 1,
                next_process_time = :nextProcessTime
            WHERE id = :id
            """;

    private static final String UPDATE_STATUS_TO_COMPLETED_SQL = """
            UPDATE pgq_simple_queue 
            SET status = 'COMPLETED'
            WHERE id = :id
            """;

    private static final String UPDATE_STATUS_TO_PENDING_SQL = """
            UPDATE pgq_simple_queue 
            SET status = 'PENDING',
                next_process_time = :nextProcessTime
            WHERE id = :id
            """;

    private static final String UPDATE_STATUS_TO_DEAD_SQL = """
            UPDATE pgq_simple_queue 
            SET status = 'DEAD'
            WHERE id = :id
            """;

    private final JdbcClient jdbcClient;

    public SimpleQueueConsumerImpl(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Override
    @Transactional
    public Optional<MessageEntity> pullMessage(String topic) {
        Objects.requireNonNull(topic, "topic must not be null");

        LocalDateTime currentTime = LocalDateTime.now();

        Optional<MessageEntity> message = jdbcClient.sql(PULL_MESSAGE_SQL)
                .param("topic", topic)
                .param("currentTime", currentTime)
                .query(MessageEntity.class)
                .optional();

        if (message.isPresent()) {
            // 更新状态为 PROCESSING
            MessageEntity messageEntity = message.get();
            updateToProcessing(messageEntity.getId());
        }

        return message;
    }

    @Override
    @Transactional
    public List<MessageEntity> pullMessages(String topic, int batchSize) {
        Objects.requireNonNull(topic, "topic must not be null");

        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }

        LocalDateTime currentTime = LocalDateTime.now();

        List<MessageEntity> messages = jdbcClient.sql(PULL_MESSAGES_SQL)
                .param("topic", topic)
                .param("currentTime", currentTime)
                .param("batchSize", batchSize)
                .query(MessageEntity.class)
                .list();

        // 批量更新状态为 PROCESSING
        for (MessageEntity message : messages) {
            updateToProcessing(message.getId());
        }

        return messages;
    }

    @Override
    @Transactional
    public void ackMessage(Long messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");

        jdbcClient.sql(UPDATE_STATUS_TO_COMPLETED_SQL)
                .param("id", messageId)
                .update();
    }

    @Override
    @Transactional
    public void nackMessage(Long messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");

        // 计算下次重试时间（延迟5分钟）
        LocalDateTime nextProcessTime = LocalDateTime.now().plusMinutes(5);

        jdbcClient.sql(UPDATE_STATUS_TO_PENDING_SQL)
                .param("id", messageId)
                .param("nextProcessTime", nextProcessTime)
                .update();
    }

    @Override
    @Transactional
    public void deadLetterMessage(Long messageId) {
        Objects.requireNonNull(messageId, "messageId must not be null");

        jdbcClient.sql(UPDATE_STATUS_TO_DEAD_SQL)
                .param("id", messageId)
                .update();
    }

    private void updateToProcessing(Long messageId) {
        // 设置下次处理时间为30分钟后（用于超时处理）
        LocalDateTime nextProcessTime = LocalDateTime.now().plusMinutes(30);

        jdbcClient.sql(UPDATE_STATUS_TO_PROCESSING_SQL)
                .param("id", messageId)
                .param("nextProcessTime", nextProcessTime)
                .update();
    }
}
