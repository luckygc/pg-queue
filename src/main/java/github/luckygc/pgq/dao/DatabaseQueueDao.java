package github.luckygc.pgq.dao;

import github.luckygc.pgq.Utils;
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

public class DatabaseQueueDao {

    private static final Logger log = LoggerFactory.getLogger(DatabaseQueueDao.class);

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

    private static final String FIND_PENDING_MESSAGES_SKIP_LOCKED = """
             select id, create_time, topic, priority, payload, attempt + 1
                    from pgq_pending_queue
                    where topic = ?
                    order by priority desc ,id
                    limit ?
                    for update skip locked
            """;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;

    public DatabaseQueueDao(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
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
        Utils.checkDurationIsPositive(processDelay);
        jdbcTemplate.update(INSERT_INTO_INVISIBLE, new InsertPsSetter(message, processDelay));
    }

    public void insertProcessLaterMessages(List<Message> messages, Duration processDelay) {
        Utils.checkMessagesNotEmpty(messages);
        Objects.requireNonNull(processDelay);
        Utils.checkDurationIsPositive(processDelay);
        jdbcTemplate.batchUpdate(INSERT_INTO_INVISIBLE, new BatchInsertPsSetter(messages, processDelay));
    }

    public List<Message> pull(String topic, int pullCount, Duration processTimeout) {
        Objects.requireNonNull(topic);
        if (pullCount < 1) {
            throw new IllegalArgumentException("pullCount必须大于0");
        }
        Objects.requireNonNull(processTimeout);
        Utils.checkDurationIsPositive(processTimeout);

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

            Long[] idArray = Utils.getIdArray(messages);
            jdbcTemplate.update(BATCH_MOVE_PENDING_MESSAGES_TO_PROCESSING, idArray, timeoutTime);
            return Objects.requireNonNull(messages);
        });

        return messageObjs == null ? Collections.emptyList() : messageObjs;
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
