package github.luckygc.pgq.api;

import github.luckygc.pgq.api.handler.BatchMessageHandler;
import github.luckygc.pgq.api.handler.SingleMessageHandler;
import java.sql.SQLException;
import java.util.Map;

public interface QueueManager {

    DatabaseQueue queue(String topic);

    ProcessingMessageManager processingMessageManager();

    DeadMessageManger deadMessageManager();

    boolean isEnablePgNotify();

    void registerMessageHandler(SingleMessageHandler messageHandler);

    void registerMessageHandler(BatchMessageHandler messageHandler);

    void start() throws SQLException;

    void stop();

    /**
     * 获取所有消息处理器的线程池状态，用于监控
     */
    Map<String, String> getThreadPoolStatus();
}
