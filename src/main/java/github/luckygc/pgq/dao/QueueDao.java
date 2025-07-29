package github.luckygc.pgq.dao;

import github.luckygc.pgq.PgmqConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class QueueDao {

    private static final Logger log = LoggerFactory.getLogger(QueueDao.class);

    private final JdbcTemplate jdbcTemplate;

    public QueueDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private static final RowMapper<String> stringMapper = (rs, ignore) -> rs.getString(1);
    private static final RowMapper<Void> emptyMapper = (rs, ignore) -> null;

    /**
     * 批量把到时间的不可见消息移入待处理队列,把处理超时任务重新移回待处理队列,并返回有可用消息的topic集合
     */
    public List<String> moveTimeoutAndVisibleMsgToPendingAndReturnMsgAvailableTopics() {
        return jdbcTemplate.query("select pgmq_move_timeout_and_visible_msg_to_pending_then_notify()", stringMapper);
    }

    public void sendNotifyWithCheckTx(String topic) {

    }

    public void sendNotify(String topic) {
        Objects.requireNonNull(topic);

        try {
            jdbcTemplate.query("select pg_notify(?, ?)", emptyMapper, PgmqConstants.TOPIC_CHANNEL, topic);
        } catch (Exception e) {
            log.warn("发送通知失败", e);
        }
    }

    public void sendNotify(List<String> topics) {
        Objects.requireNonNull(topics);
        if (topics.isEmpty()) {
            return;
        }

        List<Object[]> rows = new ArrayList<>(topics.size());
        for (String topic : topics) {
            rows.add(new Object[]{PgmqConstants.TOPIC_CHANNEL, topic});
        }

        jdbcTemplate.query("select pg_notify(?, ?)", emptyMapper, rows);
    }
}
