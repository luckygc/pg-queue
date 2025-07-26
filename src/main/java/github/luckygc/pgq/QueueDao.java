package github.luckygc.pgq;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueDao {

    private static final Logger log = LoggerFactory.getLogger(QueueDao.class);
    private static final int PGQ_ID = 199738;
    private static final int SCHEDULER_ID = 1;
    private static final String CHANNEL_NAME = "pgq_topic_channel";

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

    private static final String FIND_PENDING_MESSAGES_SKIP_LOCKED = """
             select id, create_time, topic, priority, payload, attempt + 1
                    from pgq_pending_queue
                    where topic = ?
                    order by priority desc ,id
                    limit ?
                    for update skip locked
            """;

    private static final String MOVE_PENDING_MESSAGES_TO_PROCESSING = """
            with message_to_process as (
                delete from pgq_pending_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_processing_queue
                          (id, create_time, topic, priority, payload, attempt, timeout_time)
                    select id, create_time, topic, priority, payload, attempt + 1, now() + interval '10 minutes'
                    from message_to_process
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

    private static final String MOVE_TIMEOUT_MESSAGES_TO_PENDING = """
            with message_to_pending as (
                delete from pgq_processing_queue where timeout_time <= now()
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_pending_queue
                      (id, create_time, topic, priority, payload, attempt)
                select id, create_time, topic, priority, payload, attempt from message_to_pending
            """;

    private static final String MOVE_PROCESSING_MESSAGE_TO_COMPLETE = """
            with message_to_complete as (
                delete from pgq_processing_queue where id = ?
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_complete_queue
                  (id, create_time, topic, priority, payload, attempt, complete_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_complete
            """;

    private static final String DELETE_PROCESSING_MESSAGE = """
            delete from pgq_processing_queue where id = ?
            """;

    private static final String MOVE_PROCESSING_MESSAGES_TO_COMPLETE = """
            with message_to_complete as (
                delete from pgq_processing_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_complete_queue
                  (id, create_time, topic, priority, payload, attempt, complete_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_complete
            """;

    private static final String DELETE_PROCESSING_MESSAGES = """
            delete from pgq_processing_queue where id = any(?::bigint[])
            """;

    private static final String MOVE_PROCESSING_MESSAGE_TO_DEAD = """
            with message_to_dead as (
                delete from pgq_processing_queue where id = ?
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_dead_queue
                  (id, create_time, topic, priority, payload, attempt, dead_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_dead
            """;

    private static final String MOVE_PROCESSING_MESSAGES_TO_DEAD = """
            with message_to_dead as (
                delete from pgq_processing_queue where id = any(?::bigint[])
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_dead_queue
                  (id, create_time, topic, priority, payload, attempt, dead_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_dead
            """;

    private static final String MOVE_PROCESSING_MESSAGE_TO_INVISIBLE = """
                    with message_to_retry as (
                        delete from pgq_processing_queue where id = ?
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
        this.jdbcTemplate = jdbcTemplate;
        this.txTemplate = transactionTemplate;
    }

    public void insertMessage(Message message) {
        insertMessage(message, null);
    }

    public void insertMessage(Message message, @Nullable Duration processDelay) {
        Objects.requireNonNull(message);
        checkDurationNotNegative(processDelay);

        String sql = isProcessLater(processDelay) ? INSERT_INTO_INVISIBLE : INSERT_INTO_PENDING;
        jdbcTemplate.update(sql, new InsertPsSetter(message, processDelay));
    }

    public void insertMessages(List<Message> messages) {
        insertMessages(messages, null);
    }

    public void insertMessages(List<Message> messages, @Nullable Duration processDelay) {
        Objects.requireNonNull(messages);
        checkDurationNotNegative(processDelay);

        if (messages.isEmpty()) {
            return;
        }

        String sql = isProcessLater(processDelay) ? INSERT_INTO_INVISIBLE : INSERT_INTO_PENDING;
        jdbcTemplate.batchUpdate(sql, new BatchInsertPsSetter(messages, processDelay));
    }

    private void checkDurationNotNegative(@Nullable Duration processDelay) {
        if (processDelay != null && processDelay.isNegative()) {
            throw new IllegalArgumentException("processDelay必须大于等于0");
        }
    }

    private static boolean isProcessLater(@Nullable Duration processDelay) {
        return processDelay != null && !processDelay.isZero() && !processDelay.isNegative();
    }

    /**
     * 批量把到时间的不可见消息移入待处理队列,把处理超时任务重新移回待处理队列,并发送通知提醒有可用消息
     */
    public void schedule() {
        try {
            txTemplate.executeWithoutResult(ignore -> {
                // 尝试获取事务意向锁
                boolean locked = tryLockInTx(SCHEDULER_ID);
                if (!locked) {
                    return;
                }

                // 将可见消息移动到待处理队列
                jdbcTemplate.update(MOVE_VISIBLE_MESSAGES_TO_PENDING, LocalDateTime.now());

                // 将处理超时消息移动到待处理队列
                jdbcTemplate.update(MOVE_TIMEOUT_MESSAGES_TO_PENDING);

                // 查询有可处理消息的topic并发出通知
                jdbcTemplate.update(NOTIFY_AVAILABLE_TOPIC, CHANNEL_NAME);
            });
        } catch (Throwable t) {
            log.error("调度失败", t);
        }
    }

    /**
     * 尝试获取事务级咨询锁
     */
    private boolean tryLockInTx(int objId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalArgumentException("当前事务未激活");
        }

        String sql = "SELECT pg_try_advisory_xact_lock(?, ?) AS locked";
        Boolean locked = jdbcTemplate.queryForObject(sql, boolMapper, PGQ_ID, objId);
        return Boolean.TRUE.equals(locked);
    }

    @Nullable
    public Message pull(String topic) {
        List<Message> messages = pull(topic, 1);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    public List<Message> pull(String topic, int batchSize) {
        Objects.requireNonNull(topic);

        return txTemplate.execute(ignore -> {
            List<Message> messages = jdbcTemplate.query(
                    FIND_PENDING_MESSAGES_SKIP_LOCKED,
                    messageMapper,
                    topic,
                    batchSize);

            if (messages.isEmpty()) {
                return messages;
            }

            Long[] idArray = getIdArray(messages);
            jdbcTemplate.update(MOVE_PENDING_MESSAGES_TO_PROCESSING, new Object[]{idArray});
            return messages;
        });
    }

    public void completeMessage(Message message, boolean delete) {
        Objects.requireNonNull(message);

        if (delete) {
            jdbcTemplate.update(DELETE_PROCESSING_MESSAGE, message.getId());
        } else {
            jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_COMPLETE, message.getId());
        }
    }

    public void completeMessages(List<Message> messages, boolean delete) {
        Objects.requireNonNull(messages);
        if (messages.isEmpty()) {
            return;
        }

        Long[] idArray = getIdArray(messages);

        if (delete) {
            jdbcTemplate.update(DELETE_PROCESSING_MESSAGES, new Object[]{idArray});
        } else {
            jdbcTemplate.update(MOVE_PROCESSING_MESSAGES_TO_COMPLETE, new Object[]{idArray});
        }
    }

    public void deadMessage(Message message) {
        Objects.requireNonNull(message);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_DEAD, message.getId());
    }

    public void deadMessages(List<Message> messages) {
        Objects.requireNonNull(messages);
        if (messages.isEmpty()) {
            return;
        }

        Long[] idArray = getIdArray(messages);
        jdbcTemplate.update(MOVE_PROCESSING_MESSAGES_TO_DEAD, new Object[]{idArray});
    }

    private Long[] getIdArray(List<Message> messages) {
        Objects.requireNonNull(messages);

        Long[] ids = new Long[messages.size()];
        int i = 0;
        for (Message message : messages) {
            ids[i++] = message.getId();
        }

        return ids;
    }

    public void retryMessage(Message message, @Nullable Duration processDelay) {
        Objects.requireNonNull(message);

        checkDurationNotNegative(processDelay);
        boolean isProcessLater = isProcessLater(processDelay);
        if (isProcessLater) {
            String sql = """
                    with message_to_retry as (
                        delete from pgq_processing_queue where id = $1
                        returning id, create_time, topic, priority, payload, attempt
                    )
                    insert into pgq_dead_queue
                    (id, create_time, topic, priority, payload, attempt, dead_time)
                    select
                     id, create_time, topic, priority, payload, attempt, now() from message_to_retry
                    """;
            jdbcTemplate.update(sql);
        }
    }

    @Nullable
    private static LocalDateTime computeVisibleTime(Message message, Duration processDelay) {
        if (isProcessLater(processDelay)) {
            return message.getCreateTime().plus(processDelay);
        }

        return null;
    }

    private static void insertPsSetter(PreparedStatement ps, Message message, LocalDateTime visibleTime)
            throws SQLException {
        ps.setTimestamp(1, Timestamp.valueOf(message.getCreateTime()));
        ps.setString(2, message.getTopic());
        ps.setInt(3, message.getPriority());
        ps.setString(4, message.getPayload());
        ps.setInt(5, message.getAttempt());
        if (visibleTime != null) {
            ps.setTimestamp(6, Timestamp.valueOf(visibleTime));
        }
    }

    private record InsertPsSetter(Message message, Duration processDelay) implements PreparedStatementSetter {

        @Override
        public void setValues(PreparedStatement ps) throws SQLException {
            insertPsSetter(ps, message, computeVisibleTime(message, processDelay));
        }
    }

    private record BatchInsertPsSetter(List<Message> messages, Duration processDelay) implements
            BatchPreparedStatementSetter {

        @Override
        public void setValues(PreparedStatement ps, int i) throws SQLException {
            Message message = messages.get(i);
            insertPsSetter(ps, message, computeVisibleTime(message, processDelay));
        }

        @Override
        public int getBatchSize() {
            return messages.size();
        }
    }
}
