package github.luckygc.pgq.dao;

import github.luckygc.pgq.PgmqConstants;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueManagerDao {

    private static final Logger log = LoggerFactory.getLogger(QueueManagerDao.class);

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
    private static final String FIND_AVAILABLE_TOPIC = """
            select distinct topic from pgq_pending_queue
            """;

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate txTemplate;

    public QueueManagerDao(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.txTemplate = transactionTemplate;
    }

    private static final RowMapper<Boolean> boolMapper = (rs, ignore) -> rs.getBoolean(1);
    private static final RowMapper<String> stringMapper = (rs, ignore) -> rs.getString(1);
    private static final RowMapper<Void> emptyMapper = (rs, ignore) -> null;

    /**
     * 批量把到时间的不可见消息移入待处理队列,把处理超时任务重新移回待处理队列,并返回有可用消息的topic集合
     */
    public List<String> tryHandleTimeoutAndVisibleMessagesAndReturnTopicsWithAvailableMessages() {
        List<String> finalTopics = null;

        try {
            finalTopics = txTemplate.execute(ignore -> {
                // 尝试获取锁
                Boolean locked = jdbcTemplate.queryForObject("SELECT pg_try_advisory_xact_lock(?, ?) AS locked",
                        boolMapper, PgmqConstants.PGQ_ID, PgmqConstants.SCHEDULER_ID);
                if (!Boolean.TRUE.equals(locked)) {
                    return Collections.emptyList();
                }

                // 将可见消息移动到待处理队列
                jdbcTemplate.update(MOVE_VISIBLE_MESSAGES_TO_PENDING, LocalDateTime.now());

                // 将处理超时消息移动到待处理队列
                jdbcTemplate.update(MOVE_TIMEOUT_MESSAGES_TO_PENDING);

                // 查询有可处理消息的topic并发出通知
                return jdbcTemplate.query(FIND_AVAILABLE_TOPIC, stringMapper);
            });
        } catch (Throwable t) {
            log.error("调度失败", t);
        }

        if (finalTopics == null) {
            return Collections.emptyList();
        }

        return finalTopics;
    }

    public void sendNotify(String topic) {
        Objects.requireNonNull(topic);
        jdbcTemplate.query("select pg_notify(?, ?)", emptyMapper, PgmqConstants.TOPIC_CHANNEL, topic);
    }
}
