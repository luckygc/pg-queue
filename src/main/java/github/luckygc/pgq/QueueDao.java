package github.luckygc.pgq;

import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;

public class QueueDao {

    private final JdbcClient jdbcClient;

    public QueueDao(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public void insertMessageEntity(MessageEntity messageEntity) {
        jdbcClient.sql(Sqls.INSERT_INTO_SIMPLE_QUEUE)
                .params(Utils.messageEntityToMap(messageEntity))
                .update();
    }

    public List<MessageEntity> findWaitHandleMessageEntities(String topic, long limit) {
        LocalDateTime now = LocalDateTime.now();

        List<MessageEntity> list = jdbcClient.sql(Sqls.PULL_WAIT_HANDLE_FROM_SIMPLE_QUEUE)
                .param("topic", topic)
                .param("nextProcessTime", now)
                .param("limit", limit)
                .query(RowMappers.MESSAGE_ENTITY_ROW_MAPPER)
                .list();

        if (list.isEmpty()) {
            return list;
        }

        updateMessageEntitiesStatusPending(list);

        return list;
    }

    private void updateMessageEntitiesStatusPending(List<MessageEntity> messageEntities) {
        Long[] ids = messageEntities.stream()
                .map(MessageEntity::getId)
                .toArray(Long[]::new);

        jdbcClient.sql(Sqls.UPDATE_SIMPLE_QUEUE_STATUS)
                .param("status", MessageStatus.PROCESSING.name())
                .param("ids", ids)
                .update();
    }
}
