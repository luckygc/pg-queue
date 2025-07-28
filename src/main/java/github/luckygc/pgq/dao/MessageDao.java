package github.luckygc.pgq.dao;

import java.time.LocalDateTime;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class MessageDao {

    private static final Logger log = LoggerFactory.getLogger(MessageDao.class);

    private static final String MOVE_PROCESSING_MESSAGE_TO_COMPLETE = """
            with message_to_complete as (
                delete from pgq_processing_queue where id = ?
                returning id, create_time, topic, priority, payload, attempt
            ) insert into pgq_complete_queue
                  (id, create_time, topic, priority, payload, attempt, complete_time)
            select id, create_time, topic, priority, payload, attempt, now() from message_to_complete
            """;

    // 删除处理中消息
    private static final String DELETE_PROCESSING_MESSAGE = """
            delete from pgq_processing_queue where id = ?
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

    private final JdbcTemplate jdbcTemplate;

    public MessageDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
    }

    public void deleteProcessingMsgById(Long id) {
        Objects.requireNonNull(id);

        jdbcTemplate.update(DELETE_PROCESSING_MESSAGE, id);
    }

    public void moveProcessingMsgToDeadById(Long id) {
        Objects.requireNonNull(id);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_DEAD, id);
    }

    public void moveProcessingMsgToPendingById(Long id) {
        Objects.requireNonNull(id);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_PENDING, id);
    }

    public void moveProcessingMsgToInvisibleById(Long id, LocalDateTime visibleTime) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(visibleTime);

        jdbcTemplate.update(MOVE_PROCESSING_MESSAGE_TO_INVISIBLE, id, visibleTime);
    }
}
