package github.luckygc.pgq;

import github.luckygc.pgq.model.MessageEntity;
import java.util.HashMap;
import java.util.Map;

public final class Utils {

    private Utils() {
    }

    public static Map<String, Object> messageEntityToMap(MessageEntity messageEntity) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", messageEntity.getId());
        map.put("createTime", messageEntity.getCreateTime());
        map.put("topic", messageEntity.getTopic());
        map.put("payload", messageEntity.getPayload());
        map.put("status", messageEntity.getStatus());
        map.put("priority", messageEntity.getPriority());
        map.put("nextProcessTime", messageEntity.getNextProcessTime());
        map.put("attempt", messageEntity.getAttempt());
        map.put("maxAttempt", messageEntity.getMaxAttempt());
        return map;
    }
}
