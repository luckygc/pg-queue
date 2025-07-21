package github.luckygc.pgq;

import github.luckygc.pgq.model.MessageEntity;
import java.util.List;
import java.util.Objects;
import org.springframework.jdbc.core.simple.JdbcClient;

public class QueueDao {

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
                .query(RowMappers.MESSAGE_ENTITY_ROW_MAPPER)
                .list();
    }
}
