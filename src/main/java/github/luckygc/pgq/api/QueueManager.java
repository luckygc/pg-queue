package github.luckygc.pgq.api;

import java.sql.SQLException;
import java.util.Map;

public interface QueueManager {

    DatabaseQueue queue(String topic);

    ProcessingMessageManager processingMessageManager();

    DeadMessageManger deadMessageManager();

    boolean isEnablePgNotify();

    void registerMessageHandler(SingleMessageHandler messageHandler);

    void registerMessageHandler(BatchMessageHandler messageHandler);

    /**
     * 启动
     *
     * @param loopIntervalSeconds 轮询间隔时间，单位秒，用于处理延时处理消息和处理超时消息
     */
    void start(long loopIntervalSeconds) throws SQLException;

    void stop();

    /**
     * 获取所有消息处理器的线程池状态，用于监控
     * 
     * @return Map<topic, threadPoolStatus>
     */
    Map<String, String> getThreadPoolStatus();
}
