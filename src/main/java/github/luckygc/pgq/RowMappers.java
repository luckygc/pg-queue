package github.luckygc.pgq;

import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import org.springframework.jdbc.core.RowMapper;

public final class RowMappers {

    private RowMappers() {
    }

    public static final RowMapper<MessageEntity> MESSAGE_ENTITY_ROW_MAPPER = (rs, rowNum) -> {
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
}
