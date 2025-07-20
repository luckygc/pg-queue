package github.luckygc.pgq;

import com.fasterxml.jackson.databind.ObjectMapper;
import github.luckygc.pgq.config.QueueConfig;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.springframework.jdbc.core.simple.JdbcClient;

public class QueueManager {

    private final QueueDao queueDao;
    private final ObjectMapper objectMapper;
    private final Set<QueueConfig> queueConfigs = new LinkedHashSet<>();
    private final List<MessageManager> messageManagers = new ArrayList<>();

    public QueueManager(JdbcClient jdbcClient, ObjectMapper objectMapper) {
        this.queueDao = new QueueDao(jdbcClient);
        this.objectMapper = objectMapper;
    }

    public void registerQueueConfig(QueueConfig queueConfig) {
        boolean isAdd = queueConfigs.add(queueConfig);
        if (!isAdd) {
            return;
        }

        MessageManager queueHandler = new MessageManager(queueConfig, this.queueDao,objectMapper);
        messageManagers.add(queueHandler);
    }
}
