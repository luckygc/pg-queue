package github.luckygc.pgq.dao;

import github.luckygc.pgq.Utils;
import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.model.MessageDO;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class MessageDao {

    private static final String INSERT_INTO_PENDING = """
            insert into pgq_pending_queue(create_time, topic, priority, payload, attempt)
                values(?, ?, ?, ?, ?)
            """;
    private static final String INSERT_INTO_INVISIBLE = """
            insert into pgq_invisible_queue(create_time, topic, priority, payload, attempt, visible_time)
                values(?, ?, ?, ?, ?, ?)
            """;

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

    public void insertIntoPending(MessageDO messageDO) {
        Objects.requireNonNull(messageDO);
        Object[] row = mapToArgArray(messageDO);
        jdbcTemplate.update(INSERT_INTO_PENDING, row);
    }

    public void insertIntoPending(List<MessageDO> messageDOS) {
        Utils.checkMessagesNotEmpty(messageDOS);

        List<Object[]> rows = new ArrayList<>(messageDOS.size());
        for (MessageDO messageDO : messageDOS) {
            Object[] row = mapToArgArray(messageDO);
            rows.add(row);
        }

        jdbcTemplate.batchUpdate(INSERT_INTO_PENDING, rows);
    }

    public void insertIntoInvisible(MessageDO messageDO, LocalDateTime visibleTime) {
        Objects.requireNonNull(messageDO);

        Objects.requireNonNull(visibleTime);
        Object[] row = mapToArgArray(messageDO, visibleTime);
        jdbcTemplate.update(INSERT_INTO_INVISIBLE, row);
    }

    public void insertIntoInvisible(List<MessageDO> messageDOS, LocalDateTime visibleTime) {
        Utils.checkMessagesNotEmpty(messageDOS);
        Objects.requireNonNull(visibleTime);

        List<Object[]> rows = new ArrayList<>(messageDOS.size());
        for (MessageDO messageDO : messageDOS) {
            Object[] row = mapToArgArray(messageDO, visibleTime);
            rows.add(row);
        }

        jdbcTemplate.batchUpdate(INSERT_INTO_INVISIBLE, rows);
    }

    public List<Message> getPendingMessagesAndMoveToProcessing(String topic, int maxPoll,
            LocalDateTime processTimeoutTime) {
        Objects.requireNonNull(topic);
        Utils.checkMaxPollRange(maxPoll);
        Objects.requireNonNull(processTimeoutTime);

        String sql = """
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

        return jdbcTemplate.query(sql, rowMapper, topic, maxPoll, processTimeoutTime);
    }

    public int deleteProcessingMessageById(Long id) {
        Objects.requireNonNull(id);

        return jdbcTemplate.update("delete from pgq_processing_queue where id = ?", id);
    }

    public int moveProcessingMessageToDeadById(Long id) {
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

    public int moveProcessingMessageToPendingById(Long id) {
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

    public int moveProcessingMessageToInvisibleById(Long id, LocalDateTime visibleTime) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(visibleTime);

        String sql = """
                with message_to_retry as (
                    delete from pgq_processing_queue where id = ?
                    returning id, create_time, topic, priority, payload, attempt
                )
                insert into pgq_invisible_queue
                      (id, create_time, topic, priority, payload, attempt, visible_time)
                select id, create_time, topic, priority, payload, attempt, ? from message_to_retry
                """;

        return jdbcTemplate.update(sql, id, visibleTime);
    }

    private Object[] mapToArgArray(MessageDO messageDO) {
        return new Object[]{
                messageDO.getCreateTime(),
                messageDO.getTopic(),
                messageDO.getPriority(),
                messageDO.getPayload(),
                messageDO.getAttempt()
        };
    }

    private Object[] mapToArgArray(MessageDO messageDO, LocalDateTime visibleTime) {
        return new Object[]{
                messageDO.getCreateTime(),
                messageDO.getTopic(),
                messageDO.getPriority(),
                messageDO.getPayload(),
                messageDO.getAttempt(),
                visibleTime
        };
    }
}
