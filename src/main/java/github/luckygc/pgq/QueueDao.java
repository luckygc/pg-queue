package github.luckygc.pgq;

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
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueDao {

    private static final Logger log = LoggerFactory.getLogger(QueueDao.class);

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
    private static final String MOVE_TIMEOUT_MESSAGES_TO_PENDING = """
            with message_to_pending as (
                delete from pgq_processing_queue where timeout_time <= now()
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_pending_queue
                      (id, create_time, topic, priority, payload, attempt)
                select id, create_time, topic, priority, payload, attempt from message_to_pending
            """;
    private static final String MOVE_VISIBLE_MESSAGES_TO_PENDING = """
            with message_to_pending as (
                delete from pgq_invisible_queue where visible_time <= now()
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_pending_queue
                      (id, create_time, topic, priority, payload, attempt)
                select id, create_time, topic, priority, payload, attempt from message_to_pending
            """;
    private static final String NOTIFY_AVAILABLE_TOPIC = """
            with topic_to_notify as (
                select distinct topic from pgq_pending_queue
            ) select pg_notify(?, topic) from topic_to_notify
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

    private static final RowMapper<Message> messageMapper = (rs, ignore) -> {
        Message message = new Message();
        message.setId(rs.getLong(1));
        message.setCreateTime(rs.getTimestamp(2).toLocalDateTime());
        message.setTopic(rs.getString(3));
        message.setPriority(rs.getInt(4));
        message.setPayload(rs.getString(5));
        message.setAttempt(rs.getInt(6));
        return message;
    };

    private static final RowMapper<Boolean> boolMapper = (rs, ignore) -> rs.getBoolean(1);

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;

    public QueueDao(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
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

    /**
     * 批量把到时间的不可见消息移入待处理队列,把处理超时任务重新移回待处理队列,并发送通知提醒有可用消息
     */
    public void schedule() {
        try {
            txTemplate.executeWithoutResult(ignore -> {
                // 尝试获取锁
                Boolean locked = jdbcTemplate.queryForObject("SELECT pg_try_advisory_xact_lock(?, ?) AS locked",
                        boolMapper, PgqConstants.PGQ_ID, PgqConstants.SCHEDULER_ID);
                if (!Boolean.TRUE.equals(locked)) {
                    return;
                }

                // 将可见消息移动到待处理队列
                jdbcTemplate.update(MOVE_VISIBLE_MESSAGES_TO_PENDING, LocalDateTime.now());

                // 将处理超时消息移动到待处理队列
                jdbcTemplate.update(MOVE_TIMEOUT_MESSAGES_TO_PENDING);

                // 查询有可处理消息的topic并发出通知
                jdbcTemplate.update(NOTIFY_AVAILABLE_TOPIC, PgqConstants.CHANNEL_NAME);
            });
        } catch (Throwable t) {
            log.error("调度失败", t);
        }
    }

    public List<Message> pull(String topic, int batchSize, Duration processTimeout) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(processTimeout);
        checkDurationIsPositive(processTimeout);

        LocalDateTime timeoutTime = LocalDateTime.now().plus(processTimeout);

        List<Message> messageObjs = txTemplate.execute(ignore -> {
            List<Message> messages = jdbcTemplate.query(
                    FIND_PENDING_MESSAGES_SKIP_LOCKED,
                    messageMapper,
                    topic,
                    batchSize);

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

    public void retryProcessingMessage(Message message, @Nullable Duration processDelay) {
        Objects.requireNonNull(message);

        LocalDateTime nextVisibleTime = computeNowPlusOptionalPositiveDuration(processDelay);
        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_INVISIBLE, message.getId(), nextVisibleTime);
    }

    public void retryProcessingMessages(List<Message> messages, @Nullable Duration processDelay) {
        Utils.checkMessagesNotEmpty(messages);

        LocalDateTime nextVisibleTime = computeNowPlusOptionalPositiveDuration(processDelay);
        Long[] idArray = getIdArray(messages);
        jdbcTemplate.update(BATCH_MOVE_PROCESSING_MESSAGES_TO_INVISIBLE, idArray, nextVisibleTime);
    }

    private LocalDateTime computeNowPlusOptionalPositiveDuration(@Nullable Duration duration) {
        LocalDateTime nextTime = LocalDateTime.now();
        if (duration != null) {
            checkDurationIsPositive(duration);
            nextTime = nextTime.plus(duration);
        }

        return nextTime;
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
