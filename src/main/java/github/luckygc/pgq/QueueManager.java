package github.luckygc.pgq;

import com.fasterxml.jackson.databind.ObjectMapper;
import github.luckygc.pgq.api.MessagePusher;
import github.luckygc.pgq.config.QueueConfig;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.jdbc.core.simple.JdbcClient;

public class QueueManager {

    private final QueueDao queueDao;
    private final ObjectMapper objectMapper;
    private final Map<QueueConfig<?>, MessageManager<?>> messageManagers = new ConcurrentHashMap<>();

    public QueueManager(JdbcClient jdbcClient, ObjectMapper objectMapper) {
        this.queueDao = new QueueDao(jdbcClient);
        this.objectMapper = objectMapper;
    }

    public MessagePusher<?> registerQueueConfig(QueueConfig<?> queueConfig) {
        return messageManagers.computeIfAbsent(queueConfig,
                conf -> new MessageManager<>(queueConfig, this.queueDao, objectMapper));
    }
}
