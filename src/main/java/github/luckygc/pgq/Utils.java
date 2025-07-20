package github.luckygc.pgq;

import github.luckygc.pgq.model.MessageEntity;
import github.luckygc.pgq.model.MessageStatus;
import java.util.HashMap;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;

public final class Utils {

    private Utils() {
    }

    public static RowMapper<MessageEntity> messageEntityRowMapper() {
        return (rs, rowNum) -> {
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

    public static Map<String, Object> messageEntityToMap(MessageEntity messageEntity) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", messageEntity.getId());
        map.put("createTime", messageEntity.getCreateTime());
        map.put("topic", messageEntity.getTopic());
        map.put("payload", messageEntity.getPayload());
        map.put("status", messageEntity.getStatus());
        map.put("nextProcessTime", messageEntity.getNextProcessTime());
        map.put("attempt", messageEntity.getAttempt());
        map.put("maxAttempt", messageEntity.getMaxAttempt());
        return map;
    }
}
