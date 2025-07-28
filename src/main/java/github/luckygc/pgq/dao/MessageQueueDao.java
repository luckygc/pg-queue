package github.luckygc.pgq.dao;

import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.Utils;
import github.luckygc.pgq.model.MessageDO;
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
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

public class MessageQueueDao {

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
    private static final String MOVE_PENDING_MESSAGES_TO_PROCESSING = """
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

    public static final RowMapper<MessageDO> rowMapper = (rs, ignore) -> new MessageDO.Builder()
            .id(rs.getLong(1))
            .createTime(rs.getTimestamp(2).toLocalDateTime())
            .topic(rs.getString(3))
            .priority(rs.getInt(4))
            .payload(rs.getString(5))
            .attempt(rs.getInt(6))
            .build();

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;

    public MessageQueueDao(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        this.txTemplate = Objects.requireNonNull(transactionTemplate);
    }

    public void insertMessage(MessageDO messageDO) {
        Objects.requireNonNull(messageDO);
        jdbcTemplate.update(INSERT_INTO_PENDING, new InsertPsSetter(messageDO, null));
    }

    public void insertMessages(List<MessageDO> messageDOS) {
        Utils.checkMessagesNotEmpty(messageDOS);
        jdbcTemplate.batchUpdate(INSERT_INTO_PENDING, new BatchInsertPsSetter(messageDOS, null));
    }

    public void insertProcessLaterMessage(MessageDO messageDO, Duration processDelay) {
        Objects.requireNonNull(messageDO);
        Objects.requireNonNull(processDelay);
        Utils.checkDurationIsPositive(processDelay);
        jdbcTemplate.update(INSERT_INTO_INVISIBLE, new InsertPsSetter(messageDO, processDelay));
    }

    public void insertProcessLaterMessages(List<MessageDO> messageDOS, Duration processDelay) {
        Utils.checkMessagesNotEmpty(messageDOS);
        Objects.requireNonNull(processDelay);
        Utils.checkDurationIsPositive(processDelay);
        jdbcTemplate.batchUpdate(INSERT_INTO_INVISIBLE, new BatchInsertPsSetter(messageDOS, processDelay));
    }

    public List<MessageDO> getPendingMessagesAndMoveToProcessing(String topic, int maxPoll) {
        Objects.requireNonNull(topic);
        Utils.checkMaxPollRange(maxPoll);

        List<MessageDO> finalMessageDOs = txTemplate.execute(ignore -> {
            List<MessageDO> messageDOS = jdbcTemplate.query(
                    FIND_PENDING_MESSAGES_SKIP_LOCKED,
                    rowMapper,
                    topic,
                    maxPoll);

            if (messageDOS.isEmpty()) {
                return messageDOS;
            }

            Long[] idArray = Utils.getIdArray(messageDOS);
            LocalDateTime timeoutTime = LocalDateTime.now().plus(PgmqConstants.PROCESS_TIMEOUT);
            jdbcTemplate.update(MOVE_PENDING_MESSAGES_TO_PROCESSING, idArray, timeoutTime);
            return messageDOS;
        });

        return finalMessageDOs == null ? Collections.emptyList() : finalMessageDOs;
    }


    private record InsertPsSetter(MessageDO messageDO, @Nullable Duration processDelay) implements
            PreparedStatementSetter {

        @Override
        public void setValues(@NonNull PreparedStatement ps) throws SQLException {
            ps.setTimestamp(1, Timestamp.valueOf(messageDO.getCreateTime()));
            ps.setString(2, messageDO.getTopic());
            ps.setInt(3, messageDO.getPriority());
            ps.setString(4, messageDO.getPayload());
            ps.setInt(5, messageDO.getAttempt());
            if (processDelay != null) {
                ps.setTimestamp(6, Timestamp.valueOf(messageDO.getCreateTime().plus(processDelay)));
            }
        }
    }

    private record BatchInsertPsSetter(List<MessageDO> messageDOS, @Nullable Duration processDelay) implements
            BatchPreparedStatementSetter {

        @Override
        public void setValues(@NonNull PreparedStatement ps, int i) throws SQLException {
            MessageDO messageDO = messageDOS.get(i);
            ps.setTimestamp(1, Timestamp.valueOf(messageDO.getCreateTime()));
            ps.setString(2, messageDO.getTopic());
            ps.setInt(3, messageDO.getPriority());
            ps.setString(4, messageDO.getPayload());
            ps.setInt(5, messageDO.getAttempt());
            if (processDelay != null) {
                ps.setTimestamp(6, Timestamp.valueOf(messageDO.getCreateTime().plus(processDelay)));
            }
        }

        @Override
        public int getBatchSize() {
            return messageDOS.size();
        }
    }
}
