package github.luckygc.pgq;

import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

public class QueueDao {

    private static final RowMapper<MessageEntity> MESSAGE_ENTITY_ROW_MAPPER = (rs, rowNum) -> {
        MessageEntity messageEntity = new MessageEntity();
        messageEntity.setId(rs.getLong("id"));
        messageEntity.setCreateTime(rs.getTimestamp("create_time").toLocalDateTime());
        messageEntity.setTopic(rs.getString("topic"));
        messageEntity.setPayload(rs.getString("payload"));
        messageEntity.setStatus(MessageStatus.valueOf(rs.getString("status")));
        messageEntity.setPriority(rs.getInt("priority"));
        messageEntity.setNextProcessTime(rs.getTimestamp("next_process_time").toLocalDateTime());
        messageEntity.setAttempt(rs.getInt("attempt"));
        messageEntity.setMaxAttempt(rs.getInt("max_attempt"));
        return messageEntity;
    };

    private final JdbcTemplate jdbcTemplate;
    private final NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private final JdbcClient jdbcClient;

    public QueueDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = Objects.requireNonNull(jdbcTemplate);
        this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
        this.jdbcClient = JdbcClient.create(jdbcTemplate);
    }

    public void insertMessageEntity(MessageEntity messageEntity) {
        Objects.requireNonNull(messageEntity);

        jdbcClient.sql(Sqls.INSERT)
                .params(messageEntityToMap(messageEntity))
                .update();
    }

    @SuppressWarnings("unchecked")
    public void insertMessageEntities(List<MessageEntity> messageEntities) {
        Objects.requireNonNull(messageEntities);
        if (messageEntities.isEmpty()) {
            return;
        }

        Map<String, ?>[] array = messageEntities.stream().map(QueueDao::messageEntityToMap)
                .toArray(Map[]::new);

        namedParameterJdbcTemplate.batchUpdate(Sqls.INSERT, array);
    }

    public void updateMessageEntity(MessageEntity messageEntity) {
        jdbcClient.sql(Sqls.UPDATE)
                .params(messageEntityToMap(messageEntity))
                .update();
    }

    public List<MessageEntity> findWaitHandleMessageEntities(String topic, long limit) {
        return jdbcClient.sql(Sqls.PULL)
                .param("topic", topic)
                .param("limit", limit)
                .query(MESSAGE_ENTITY_ROW_MAPPER)
                .list();
    }

    public long deleteByStatus(String topic, MessageStatus status) {
        return jdbcClient.sql("delete from pgq_message where topic = :topic and status = :status")
                .param("topic", topic)
                .param("status", status.name())
                .update();
    }

    private static Map<String, Object> messageEntityToMap(MessageEntity messageEntity) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", messageEntity.getId());
        map.put("createTime", messageEntity.getCreateTime());
        map.put("topic", messageEntity.getTopic());
        map.put("payload", messageEntity.getPayload());
        map.put("status", messageEntity.getStatus().name());
        map.put("priority", messageEntity.getPriority());
        map.put("nextProcessTime", messageEntity.getNextProcessTime());
        map.put("attempt", messageEntity.getAttempt());
        map.put("maxAttempt", messageEntity.getMaxAttempt());
        return map;
    }
}
