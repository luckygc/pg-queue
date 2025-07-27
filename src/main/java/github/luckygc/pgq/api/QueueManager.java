package github.luckygc.pgq.api;

import java.sql.SQLException;
import org.jspecify.annotations.Nullable;

public interface QueueManager {

    DatabaseQueue queue(String topic);

    MessageManager messageManager();

    DeadMessageManger deadMessageManager();

    void registerListener(QueueListener messageListener);

    @Nullable
    QueueListener listener(String topic);

    void registerMessageHandler(SingleMessageHandler messageHandler);

    void registerMessageHandler(BatchMessageHandler messageHandler);

    void startListen() throws SQLException;

    void stopListen();
}
