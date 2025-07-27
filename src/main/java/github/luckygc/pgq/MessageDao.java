package github.luckygc.pgq;

import github.luckygc.pgq.model.Message;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.support.TransactionTemplate;

public class MessageDao {

    private static final Logger log = LoggerFactory.getLogger(MessageDao.class);

    // 插入
    private static final String INSERT_INTO_PENDING = """
            insert into pgq_pending_queue
                (create_time, topic, priority, payload, attempt)
                values(?, ?, ?, ?, ?)
            """;
    private static final String INSERT_INTO_INVISIBLE = """
            insert into pgq_invisible_queue
                (create_time, topic, priority, payload, attempt, visible_time)
                values(?, ?, ?, ?, ?, ?)
            """;

    /**
     * 移动待处理消息到处理中队列
     */
    private static final String BATCH_MOVE_PENDING_MESSAGES_TO_PROCESSING = """
            with message_to_process as (
                delete from pgq_pending_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_processing_queue
                          (id, create_time, topic, priority, payload, attempt, timeout_time)
                    select id, create_time, topic, priority, payload, attempt + 1, ?
                    from message_to_process
            """;

    // schedule
    private static final String FIND_PENDING_MESSAGES_SKIP_LOCKED = """
             select id, create_time, topic, priority, payload, attempt + 1
                    from pgq_pending_queue
                    where topic = ?
                    order by priority desc ,id
                    limit ?
                    for update skip locked
            """;


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

    public void insertMessage(Message message) {
        Objects.requireNonNull(message);
        jdbcTemplate.update(INSERT_INTO_PENDING, new InsertPsSetter(message, null));
    }

    public void insertMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);
        jdbcTemplate.batchUpdate(INSERT_INTO_PENDING, new BatchInsertPsSetter(messages, null));
    }

    public void insertProcessLaterMessage(Message message, Duration processDelay) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(processDelay);
        checkDurationIsPositive(processDelay);
        jdbcTemplate.update(INSERT_INTO_INVISIBLE, new InsertPsSetter(message, processDelay));
    }

    public void insertProcessLaterMessages(List<Message> messages, Duration processDelay) {
        Utils.checkMessagesNotEmpty(messages);
        Objects.requireNonNull(processDelay);
        checkDurationIsPositive(processDelay);
        jdbcTemplate.batchUpdate(INSERT_INTO_INVISIBLE, new BatchInsertPsSetter(messages, processDelay));
    }

    private void checkDurationIsPositive(Duration duration) {
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("duration必须大于0秒");
        }
    }


    public List<Message> pull(String topic, int pullCount, Duration processTimeout) {
        Objects.requireNonNull(topic);
        if (pullCount < 1) {
            throw new IllegalArgumentException("pullCount必须大于0");
        }
        Objects.requireNonNull(processTimeout);
        checkDurationIsPositive(processTimeout);

        LocalDateTime timeoutTime = LocalDateTime.now().plus(processTimeout);

        List<Message> messageObjs = txTemplate.execute(ignore -> {
            List<Message> messages = jdbcTemplate.query(
                    FIND_PENDING_MESSAGES_SKIP_LOCKED,
                    Message.rowMapper,
                    topic,
                    pullCount);

            if (messages.isEmpty()) {
                return messages;
            }

            Long[] idArray = getIdArray(messages);
            jdbcTemplate.update(BATCH_MOVE_PENDING_MESSAGES_TO_PROCESSING, idArray, timeoutTime);
            return Objects.requireNonNull(messages);
        });

        return messageObjs == null ? Collections.emptyList() : messageObjs;
    }

    public void deleteProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(DELETE_PROCESSING_MESSAGE, message.getId());
    }

    public void deleteProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = getIdArray(messages);
        jdbcTemplate.update(BATCH_DELETE_PROCESSING_MESSAGES, new Object[]{idArray});
    }

    public void completeProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_COMPLETE, message.getId());
    }

    public void completeProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_COMPLETE, new Object[]{idArray});
    }

    public void deadProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_DEAD, message.getId());
    }

    public void deadProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_DEAD, new Object[]{idArray});
    }

    public void retryProcessingMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_PENDING, message.getId());
    }

    public void retryProcessingMessage(Message message, Duration processDelay) {
        Objects.requireNonNull(message);
        Objects.requireNonNull(processDelay);
        checkDurationIsPositive(processDelay);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_INVISIBLE, message.getId(),
                LocalDateTime.now().plus(processDelay));
    }

    public void retryProcessingMessages(List<Message> messages) {
        Utils.checkMessagesNotEmpty(messages);

        Long[] idArray = getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_PENDING, new Object[]{idArray});
    }

    public void retryProcessingMessages(List<Message> messages, Duration processDelay) {
        Utils.checkMessagesNotEmpty(messages);
        Objects.requireNonNull(processDelay);
        checkDurationIsPositive(processDelay);

        Long[] idArray = getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_INVISIBLE, idArray,
                LocalDateTime.now().plus(processDelay));
    }

    private Long[] getIdArray(List<Message> messages) {
        Long[] ids = new Long[messages.size()];
        int i = 0;
        for (Message message : messages) {
            ids[i++] = Objects.requireNonNull(message.getId());
        }

        return ids;
    }

    private static void insertPsSetter(PreparedStatement ps, Message message)
            throws SQLException {
        insertPsSetter(ps, message, null);
    }

    private static void insertPsSetter(PreparedStatement ps, Message message, @Nullable LocalDateTime visibleTime)
            throws SQLException {
        ps.setTimestamp(1, Timestamp.valueOf(Objects.requireNonNull(message.getCreateTime())));
        ps.setString(2, message.getTopic());
        ps.setInt(3, Objects.requireNonNull(message.getPriority()));
        ps.setString(4, message.getPayload());
        ps.setInt(5, Objects.requireNonNull(message.getAttempt()));
        if (visibleTime != null) {
            ps.setTimestamp(6, Timestamp.valueOf(visibleTime));
        }
    }

    private record InsertPsSetter(Message message, @Nullable Duration processDelay) implements PreparedStatementSetter {

        @Override
        public void setValues(@NonNull PreparedStatement ps) throws SQLException {
            if (processDelay == null) {
                insertPsSetter(ps, message);
                return;
            }

            insertPsSetter(ps, message, Objects.requireNonNull(message.getCreateTime()).plus(processDelay));
        }
    }

    private record BatchInsertPsSetter(List<Message> messages, @Nullable Duration processDelay) implements
            BatchPreparedStatementSetter {

        @Override
        public void setValues(@NonNull PreparedStatement ps, int i) throws SQLException {
            Message message = messages.get(i);
            if (processDelay == null) {
                insertPsSetter(ps, message);
                return;
            }

            insertPsSetter(ps, message, Objects.requireNonNull(message.getCreateTime()).plus(processDelay));
        }

        @Override
        public int getBatchSize() {
            return messages.size();
        }
    }
}
