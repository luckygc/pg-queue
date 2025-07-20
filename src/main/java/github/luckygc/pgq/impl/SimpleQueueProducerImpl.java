package github.luckygc.pgq.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.PublishException;
import github.luckygc.pgq.api.QueueProducer;
import java.util.Objects;
import org.springframework.jdbc.core.simple.JdbcClient;

public class SimpleQueueProducerImpl implements QueueProducer {

    private static final String PUBLISH_SQL = """
            INSERT INTO pgq_simple_queue (
                create_time,
                payload,
                topic,
                status,
                next_process_time,
                attempt,
                max_attempt
            ) VALUES (
                :createTime,
                :payload,
                :topic,
                :status,
                :nextProcessTime,
                :attempt,
                :maxAttempt
            )
            """;

    private final ObjectMapper objectMapper;

    private final JdbcClient jdbcClient;

    public SimpleQueueProducerImpl(ObjectMapper objectMapper, JdbcClient jdbcClient) {
        this.objectMapper = objectMapper;
        this.jdbcClient = jdbcClient;
    }

    @Override
    public void publish(Object message) {
        Objects.requireNonNull(message, "message must not be null");

        Object finalMessage = message;

        if (!(finalMessage instanceof String)) {
            try {
                finalMessage = objectMapper.writeValueAsString(message);
            } catch (JsonProcessingException e) {
                throw new PublishException("message序列化失败", e);
            }
        }

        MessageEntity messageEntity = MessageEntity.of();

        jdbcClient.sql(PUBLISH_SQL)
                .param("createTime", messageEntity.getCreateTime())
                .param("payload", messageEntity.getPayload())
                .param("topic", messageEntity.getTopic())
                .param("status", messageEntity.getStatus())
                .param("nextProcessTime", messageEntity.getNextProcessTime())
                .param("attempt", messageEntity.getAttempt())
                .param("maxAttempt", messageEntity.getMaxAttempt())
                .update();
    }
}
