package github.luckygc.pgq;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueDao {

    private static final Logger log = LoggerFactory.getLogger(QueueDao.class);
    private static final long PGQ_ID = 199738;
    private static final long SCHEDULER_ID = 1;
    private static final String CHANNEL_NAME = "pgq_channel";

    private static final String NEW_INSERT_INTO_PENDING = """
            insert into pgq_pending_queue
                (create_time, topic, priority, payload, attempt)
                values($1, $2, $3, $4, $5)
            """;

    private static final String NEW_INSERT_INTO_INVISIBLE = """
            insert into pgq_invisible_queue
                (create_time, topic, priority, payload, attempt, visible_time)
                values($1, $2, $3, $4, $5, $6)
            """;

    private static final String NEW_BATCH_INSERT_INTO_PENDING = """
            insert into pgq_pending_queue
                (create_time, topic, priority, payload, attempt)
                select * from unnest($1, $2, $3, $4, $5) as t
                (create_time, topic, priority, payload, attempt)
            """;

    private static final String NEW_BATCH_INSERT_INTO_INVISIBLE = """
            insert into pgq_invisible_queue
                (create_time, topic, priority, payload, attempt, visible_time)
                select * from unnest($1, $2, $3, $4, $5, $6) as t
                (create_time, topic, priority, payload, attempt, visible_time)
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
        checkProcessDelay(processDelay);

        jdbcTemplate.execute(insertPsCreator(message, processDelay), PreparedStatement::executeUpdate);
    }

    private PreparedStatementCreator insertPsCreator(Message message, Duration processDelay) {
        return con -> {
            String sql = NEW_INSERT_INTO_PENDING;
            boolean isProcessLater = isProcessLater(processDelay);
            if (isProcessLater) {
                sql = NEW_INSERT_INTO_INVISIBLE;
            }

            PreparedStatement ps = con.prepareStatement(sql);

            ps.setTimestamp(1, Timestamp.valueOf(message.getCreateTime()));
            ps.setString(2, message.getTopic());
            ps.setInt(3, message.getPriority());
            ps.setString(4, message.getPayload());
            ps.setInt(5, message.getAttempt());
            if (isProcessLater) {
                ps.setTimestamp(6, Timestamp.valueOf(message.getCreateTime().plus(processDelay)));
            }

            return ps;
        };
    }

    public void insertMessages(List<Message> messages) {
        insertMessages(messages, null);
    }

    public void insertMessages(List<Message> messages, @Nullable Duration processDelay) {
        Objects.requireNonNull(messages);

        if (messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            insertMessage(messages.get(0), processDelay);
            return;
        }

        checkProcessDelay(processDelay);
        jdbcTemplate.execute(batchInsertPsCreator(messages, processDelay), PreparedStatement::executeUpdate);
    }

    private void checkProcessDelay(@Nullable Duration processDelay) {
        if (processDelay != null && processDelay.isNegative()) {
            throw new IllegalArgumentException("processDelay必须大于0");
        }
    }

    private boolean isProcessLater(@Nullable Duration processDelay) {
        return processDelay != null && !processDelay.isZero();
    }

    private PreparedStatementCreator batchInsertPsCreator(List<Message> messages, @Nullable Duration processDelay) {
        return con -> {
            String sql = NEW_BATCH_INSERT_INTO_PENDING;
            boolean isProcessLater = isProcessLater(processDelay);
            if (isProcessLater) {
                sql = NEW_BATCH_INSERT_INTO_INVISIBLE;
            }

            PreparedStatement ps = con.prepareStatement(sql);

            int size = messages.size();
            Timestamp[] createTimes = new Timestamp[size];
            String[] topics = new String[size];
            Integer[] priorities = new Integer[size];
            String[] payloads = new String[size];
            Integer[] attempts = new Integer[size];
            Timestamp[] visibleTimes = new Timestamp[isProcessLater ? size : 0];
            int i = 0;
            for (Message message : messages) {
                createTimes[i] = Timestamp.valueOf(message.getCreateTime());
                topics[i] = message.getTopic();
                priorities[i] = message.getPriority();
                payloads[i] = message.getPayload();
                attempts[i] = message.getAttempt();
                if (isProcessLater) {
                    visibleTimes[i] = Timestamp.valueOf(message.getCreateTime().plus(processDelay));
                }
                i++;
            }

            ps.setArray(1, con.createArrayOf("timestamp", createTimes));
            ps.setArray(2, con.createArrayOf("varchar", topics));
            ps.setArray(3, con.createArrayOf("integer", priorities));
            ps.setArray(4, con.createArrayOf("varchar", payloads));
            ps.setArray(5, con.createArrayOf("integer", attempts));
            if (isProcessLater) {
                ps.setArray(6, con.createArrayOf("timestamp", visibleTimes));
            }

            return ps;
        };
    }

    /**
     * 批量把到时间的不可见消息移入待处理队列,把处理超时任务重新移回待处理队列,并发送通知提醒有可用消息
     */
    public void schedule() {
        try {
            txTemplate.executeWithoutResult(ignore -> scheduleInternal());
        } catch (Throwable t) {
            log.error("调度失败", t);
        }
    }

    private void scheduleInternal() {
        // 尝试获取锁
        boolean locked = tryLockInTx(SCHEDULER_ID);
        if (!locked) {
            return;
        }

        // 将当前可见消息插入到待处理队列并删除
        {
            String sql = """
                    with message_to_pending as (
                        delete from pgq_invisible_queue where visible_time <= $1
                        returning id, create_time, topic, priority, payload, attempt
                    ) insert into pgq_pending_queue
                              (id, create_time, topic, priority, payload, attempt)
                        select id, create_time, topic, priority, payload, attempt from message_to_pending
                    """;
            jdbcTemplate.update(sql, LocalDateTime.now());
        }

        // 查询待处理topic并发出通知
        {
            String sql = """
                    with topic_to_notify as (
                        select distinct topic from pgq_pending_queue
                    ) select pg_notify($1, topic) from topic_to_notify
                    """;
            jdbcTemplate.update(sql, CHANNEL_NAME);
        }
    }

    private boolean tryLockInTx(long objId) {
        String sql = "SELECT pg_try_advisory_xact_lock($1, $2) AS locked";
        Boolean locked = jdbcTemplate.queryForObject(sql, boolMapper, PGQ_ID, objId);
        return Boolean.TRUE.equals(locked);
    }

    @Nullable
    public Message pull(String topic) {
        Objects.requireNonNull(topic);

        List<Message> messages = pull(topic, 1);
        if (messages.isEmpty()) {
            return null;
        }

        return messages.get(0);
    }

    public List<Message> pull(String topic, int batchSize) {
        Objects.requireNonNull(topic);

        String sql = """
                with message_to_process as (
                    select *
                    from pgq_pending_queue
                    where topic = $1
                    order by priority desc ,id
                    limit $2
                    for update skip locked
                ), insert_op as (
                    insert into pgq_processing_queue
                          (id, create_time, topic, priority, payload, attempt, process_time)
                    select id, create_time, topic, priority, payload, attempt + 1, now() from message_to_process
                ) delete from pgq_pending_queue where id in (select w.id from message_to_process w)
                returning id, create_time, topic, priority, payload, attempt + 1
                """;
        return jdbcTemplate.query(sql, messageMapper, topic, batchSize);
    }

    public void completeMessage(Message message, boolean delete) {
        Objects.requireNonNull(message);

        if (delete) {
            jdbcTemplate.update("delete from pgq_processing_queue where id = $1", message.getId());
        } else {
            jdbcTemplate.update("""
                    with message_to_complete as (
                        delete from pgq_processing_queue where id = $1
                        returning id, create_time, topic, priority, payload, attempt
                    ) insert into pgq_complete_queue
                          (id, create_time, topic, priority, payload, attempt, complete_time)
                    select id, create_time, topic, priority, payload, attempt, now() from message_to_complete
                    """, message.getId());
        }
    }

    public void completeMessage(List<Message> messages, boolean delete) {
        Objects.requireNonNull(messages);
        if (messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            completeMessage(messages.get(0), delete);
            return;
        }

        Long[] idArray = getIdArray(messages);

        if (delete) {
            jdbcTemplate.update("delete from pgq_processing_queue where id = any($1::bigint[])", new Object[]{idArray});
        } else {
            String sql = """
                    with message_to_complete as (
                        delete from pgq_processing_queue where id = any($1::bigint[])
                        returning id, create_time, topic, priority, payload, attempt
                    ) insert into pgq_complete_queue
                           (id, create_time, topic, priority, payload, attempt, complete_time)
                    select id, create_time, topic, priority, payload, attempt, now() from message_to_complete
                    """;
            jdbcTemplate.update(sql, new Object[]{idArray});
        }
    }

    public void deadMessage(Message message) {
        Objects.requireNonNull(message);

        String sql = """
                with message_to_dead as (
                    delete from pgq_processing_queue where id = $1
                    returning id, create_time, topic, priority, payload, attempt
                )
                insert into pgq_dead_queue
                (id, create_time, topic, priority, payload, attempt, dead_time)
                select
                 id, create_time, topic, priority, payload, attempt, now() from message_to_dead
                """;
        jdbcTemplate.update(sql, message.getId());
    }

    public void deadMessage(List<Message> messages) {
        Objects.requireNonNull(messages);
        if (messages.isEmpty()) {
            return;
        }

        if (messages.size() == 1) {
            deadMessage(messages.get(0));
            return;
        }

        Long[] idArray = getIdArray(messages);
        String sql = """
                with message_to_dead as (
                    delete from pgq_processing_queue where id = any($1::bigint[])
                    returning id, create_time, topic, priority, payload, attempt
                )
                insert into pgq_dead_queue
                (id, create_time, topic, priority, payload, attempt, dead_time)
                select
                 id, create_time, topic, priority, payload, attempt, now() from message_to_dead
                """;

        jdbcTemplate.update(sql, new Object[]{idArray});
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

        checkProcessDelay(processDelay);
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
}
