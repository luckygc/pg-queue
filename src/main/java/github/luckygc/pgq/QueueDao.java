package github.luckygc.pgq;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

public class QueueDao {

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

    private final JdbcTemplate jdbcTemplate;

    public QueueDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
            boolean isProcessLater = processDelay != null && !processDelay.isZero();
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

    private PreparedStatementCreator batchInsertPsCreator(List<Message> messages, @Nullable Duration processDelay) {
        return con -> {
            String sql = NEW_BATCH_INSERT_INTO_PENDING;
            boolean isProcessLater = processDelay != null && !processDelay.isZero();
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
     * 批量把到时间的不可见消息移入待处理队列
     */
    public void schedule() {
        String sql = """
                WITH lock_result AS (
                  SELECT pg_try_advisory_xact_lock(654321) AS locked
                ),
                message_to_pending as (
                    delete from pgq_invisible_queue where visible_time <= $1 and (select locked from lock_result)
                    returning id, create_time, topic, priority, payload, attempt
                ),
                insert_op as (
                    insert into pgq_pending_queue
                          (id, create_time, topic, priority, payload, attempt)
                    select id, create_time, topic, priority, payload, attempt from message_to_pending
                ) select pg_notify('__pgq_queue__', topic) from pgq_pending_queue
                       where (select locked from lock_result) group by topic
                """;
        jdbcTemplate.update(sql, LocalDateTime.now());
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
        if (batchSize < 1 || batchSize > 5000) {
            throw new IllegalArgumentException("batch必须在[1-5000]之间");
        }

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
                    select id, create_time, topic, priority, payload, attempt, now() from message_to_process
                ) delete from pgq_pending_queue where id in (select w.id from message_to_process w)
                returning id, create_time, topic, priority, payload, attempt
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

    private PreparedStatement createPsAndSetIdArray(Connection con, String sql, List<Message> messages)
            throws SQLException {
        PreparedStatement ps = con.prepareStatement(sql);
        Long[] ids = new Long[messages.size()];
        int i = 0;
        for (Message message : messages) {
            ids[i++] = message.getId();
        }

        ps.setArray(1, con.createArrayOf("bigint", ids));

        return ps;
    }
}
