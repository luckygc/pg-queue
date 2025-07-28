package github.luckygc.pgq.dao;

import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.Utils;
import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.model.MessageDO;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
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

public class MessageDao {

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
    private static final String SELECT_AND_MOVE_PENDING_MESSAGES_TO_PROCESSING = """
            with message_to_process as (
                select id, create_time, topic, priority, payload, attempt
                    from pgq_pending_queue
                    where topic = ?
                    order by priority desc ,id
                    limit ?
                    for update skip locked
            ), delete_from_pending as (
                delete from pgq_pending_queue where id in (select id from message_to_process)
            ), insert_into_processing as (
                insert into pgq_processing_queue
                          (id, create_time, topic, priority, payload, attempt, timeout_time)
                    select id, create_time, topic, priority, payload, attempt + 1, ? from message_to_process
            ) select id, create_time, topic, priority, payload, attempt + 1 from message_to_process
            """;
    private static final Logger log = LoggerFactory.getLogger(MessageDao.class);

    private final JdbcTemplate jdbcTemplate;
    public final RowMapper<Message> rowMapper;

    public MessageDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        this.rowMapper = (rs, ignore) -> new Message.Builder()
                .id(rs.getLong(1))
                .createTime(rs.getTimestamp(2).toLocalDateTime())
                .topic(rs.getString(3))
                .priority(rs.getInt(4))
                .payload(rs.getString(5))
                .attempt(rs.getInt(6))
                .messageDao(this)
                .build();
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

    public List<Message> getPendingMessagesAndMoveToProcessing(String topic, int maxPoll) {
        Objects.requireNonNull(topic);
        Utils.checkMaxPollRange(maxPoll);

        LocalDateTime timeoutTime = LocalDateTime.now().plus(PgmqConstants.PROCESS_TIMEOUT);

        return jdbcTemplate.query(SELECT_AND_MOVE_PENDING_MESSAGES_TO_PROCESSING, rowMapper, topic, maxPoll,
                timeoutTime);
    }

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


    public int deleteProcessingMsgById(Long id) {
        Objects.requireNonNull(id);

        return jdbcTemplate.update("delete from pgq_processing_queue where id = ?", id);
    }

    public int moveProcessingMsgToDeadById(Long id) {
        Objects.requireNonNull(id);

        String sql = """
                with message_to_dead as (
                    delete from pgq_processing_queue where id = ?
                    returning id, create_time, topic, priority, payload, attempt
                ) insert into pgq_dead_queue
                      (id, create_time, topic, priority, payload, attempt, dead_time)
                select id, create_time, topic, priority, payload, attempt, now() from message_to_dead
                """;

        return jdbcTemplate.update(sql, id);
    }

    public int moveProcessingMsgToPendingById(Long id) {
        Objects.requireNonNull(id);

        String sql = """
                with message_to_retry as (
                    delete from pgq_processing_queue where id = ?
                    returning id, create_time, topic, priority, payload, attempt
                )
                insert into pgq_pending_queue
                      (id, create_time, topic, priority, payload, attempt)
                select id, create_time, topic, priority, payload, attempt from message_to_retry
                """;

        return jdbcTemplate.update(sql, id);
    }

    public void moveProcessingMsgToInvisibleById(Long id, LocalDateTime visibleTime) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(visibleTime);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_INVISIBLE, id, visibleTime);
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
