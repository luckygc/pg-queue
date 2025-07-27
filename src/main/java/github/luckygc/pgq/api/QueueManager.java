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

    void start() throws SQLException;

    void stop();
}
