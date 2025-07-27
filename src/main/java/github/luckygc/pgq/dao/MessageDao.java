package github.luckygc.pgq.dao;

import github.luckygc.pgq.Utils;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class MessageDao {

    private static final Logger log = LoggerFactory.getLogger(MessageDao.class);

    // 插入



    private static final String MOVE_PROCESSING_MESSAGE_TO_COMPLETE = """
            with message_to_complete as (
                delete from pgq_processing_queue where id = ?
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_complete_queue
                  (id, create_time, topic, priority, payload, attempt, complete_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_complete
            """;
    private static final String BATCH_MOVE_PROCESSING_MESSAGES_TO_COMPLETE = """
            with message_to_complete as (
                delete from pgq_processing_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_complete_queue
                  (id, create_time, topic, priority, payload, attempt, complete_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_complete
            """;

    // 删除处理中消息
    private static final String DELETE_PROCESSING_MESSAGE = """
            delete from pgq_processing_queue where id = ?
            """;
    private static final String BATCH_DELETE_PROCESSING_MESSAGES = """
            delete from pgq_processing_queue where id = any(?::bigint[])
            """;

    // 移动处理中消息到死信队列
    private static final String MOVE_PROCESSING_MESSAGE_TO_DEAD = """
            with message_to_dead as (
                delete from pgq_processing_queue where id = ?
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_dead_queue
                  (id, create_time, topic, priority, payload, attempt, dead_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_dead
            """;
    private static final String BATCH_MOVE_PROCESSING_MESSAGES_TO_DEAD = """
            with message_to_dead as (
                delete from pgq_processing_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_dead_queue
                  (id, create_time, topic, priority, payload, attempt, dead_time)
            select id, create_time, topic, priority, payload, attempt, ? from message_to_dead
            """;

    // 移动处理中消息到不可见队列等待重试
    private static final String MOVE_PROCESSING_MESSAGE_TO_INVISIBLE = """
            with message_to_retry as (
                delete from pgq_processing_queue where id = ?
                returning id, create_time, topic, priority, payload, attempt
            )
            insert into pgq_invisible_queue
                  (id, create_time, topic, priority, payload, attempt, visible_time)
            select id, create_time, topic, priority, payload, attempt, ? from message_to_retry
            """;
    private static final String BATCH_MOVE_PROCESSING_MESSAGES_TO_INVISIBLE = """
            with message_to_retry as (
                delete from pgq_processing_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            )
            insert into pgq_invisible_queue
                  (id, create_time, topic, priority, payload, attempt, visible_time)
            select id, create_time, topic, priority, payload, attempt, ? from message_to_retry
            """;

    // 移动到处理中消息到待处理队列重试
    private static final String MOVE_PROCESSING_MESSAGE_TO_PENDING = """
            with message_to_retry as (
                delete from pgq_processing_queue where id = ?
                returning id, create_time, topic, priority, payload, attempt
            )
            insert into pgq_pending_queue
                  (id, create_time, topic, priority, payload, attempt)
            select id, create_time, topic, priority, payload, attempt from message_to_retry
            """;
    private static final String BATCH_MOVE_PROCESSING_MESSAGES_TO_PENDING = """
            with message_to_retry as (
                delete from pgq_processing_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            )
            insert into pgq_pending_queue
                  (id, create_time, topic, priority, payload, attempt)
            select id, create_time, topic, priority, payload, attempt from message_to_retry
            """;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;

    public MessageDao(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        this.txTemplate = Objects.requireNonNull(transactionTemplate);
    }



    public void deleteProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(DELETE_PROCESSING_MESSAGE, message.getId());
    }

    public void deleteProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = Utils.getIdArray(messages);
        jdbcTemplate.update(BATCH_DELETE_PROCESSING_MESSAGES, new Object[]{idArray});
    }

    public void completeProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_COMPLETE, message.getId());
    }

    public void completeProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = Utils.getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_COMPLETE, new Object[]{idArray});
    }

    public void deadProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_DEAD, message.getId());
    }

    public void deadProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = Utils.getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_DEAD, new Object[]{idArray});
    }

    public void retryProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_PENDING, message.getId());
    }

    public void retryProcessingMessage(Message message, Duration processDelay) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(processDelay);
        Utils.checkDurationIsPositive(processDelay);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_INVISIBLE, message.getId(),
                LocalDateTime.now().plus(processDelay));
    }

    public void retryProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = Utils.getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_PENDING, new Object[]{idArray});
    }

    public void retryProcessingMessages(List<Message> messages, Duration processDelay) {
        Utils.checkMessagesNotEmpty(messages);
        Objects.requireNonNull(processDelay);
        Utils.checkDurationIsPositive(processDelay);

        Long[] idArray = Utils.getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_INVISIBLE, idArray,
                LocalDateTime.now().plus(processDelay));
    }


}
