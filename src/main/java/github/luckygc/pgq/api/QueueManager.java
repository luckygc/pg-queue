package github.luckygc.pgq.api;

import java.sql.SQLException;
import org.jspecify.annotations.Nullable;

public interface QueueManager {

    DatabaseQueue queue(String topic);

    MessageManager messageManager();

    DeadMessageManger deadMessageManager();

    boolean isEnablePgNotify();

    void registerListener(QueueListener messageListener);

    @Nullable
    QueueListener listener(String topic);

    void registerMessageHandler(SingleMessageHandler messageHandler);

    void registerMessageHandler(BatchMessageHandler messageHandler);

    /**
     * 启动
     *
     * @param loopIntervalSeconds 轮询间隔时间，单位秒，用于处理延时处理消息和处理超时消息
     */
    void start(long loopIntervalSeconds) throws SQLException;

    void stop();
}
