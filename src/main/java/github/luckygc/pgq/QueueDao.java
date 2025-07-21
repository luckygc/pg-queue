package github.luckygc.pgq;

import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import java.util.List;
import java.util.Objects;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;

public class QueueDao {

    private static final RowMapper<MessageEntity> MESSAGE_ENTITY_ROW_MAPPER = (rs, rowNum) -> {
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setId(rs.getLong("id"));
        messageEntity.setCreateTime(rs.getTimestamp("create_time").toLocalDateTime());
        messageEntity.setTopic(rs.getString("topic"));
        messageEntity.setPayload(rs.getString("payload"));
        messageEntity.setStatus(MessageStatus.valueOf(rs.getString("status")));
        messageEntity.setNextProcessTime(rs.getTimestamp("next_process_time").toLocalDateTime());
        messageEntity.setAttempt(rs.getInt("attempt"));
        messageEntity.setMaxAttempt(rs.getInt("max_attempt"));
        return messageEntity;
    };

    private final JdbcClient jdbcClient;

    public QueueDao(JdbcClient jdbcClient) {
        this.jdbcClient = Objects.requireNonNull(jdbcClient);
    }

    public void insertMessageEntity(MessageEntity messageEntity) {
        jdbcClient.sql(Sqls.INSERT)
                .params(Utils.messageEntityToMap(messageEntity))
                .update();
    }

    public void updateMessageEntity(MessageEntity messageEntity) {
        jdbcClient.sql(Sqls.UPDATE)
                .params(Utils.messageEntityToMap(messageEntity))
                .update();
    }

    public List<MessageEntity> findWaitHandleMessageEntities(String topic, long limit) {
        return jdbcClient.sql(Sqls.PULL)
                .param("topic", topic)
                .param("limit", limit)
                .query(MESSAGE_ENTITY_ROW_MAPPER)
                .list();
    }
}
