package github.luckygc.pgq.dao;

import github.luckygc.pgq.PgmqConstants;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueManagerDao {

    private static final Logger log = LoggerFactory.getLogger(QueueManagerDao.class);

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
    public List<String> moveTimeoutAndVisibleMsgToPendingAndReturnMsgAvailableTopics() {
        return jdbcTemplate.query("select pgmq_move_timeout_and_visible_msg_to_pending_then_notify()", stringMapper);
    }

    public void sendNotify(String topic) {
        Objects.requireNonNull(topic);
        jdbcTemplate.query("select pg_notify(?, ?)", emptyMapper, PgmqConstants.TOPIC_CHANNEL, topic);
    }
}
