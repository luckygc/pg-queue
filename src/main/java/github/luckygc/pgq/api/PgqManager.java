package github.luckygc.pgq.api;

import java.sql.SQLException;
import org.jspecify.annotations.Nullable;

public interface PgqManager {

    QueueManager register(String topic);

    QueueManager register(String topic, QueueListener messageListener);

    QueueManager register(String topic, SingleMessageHandler messageHandler);

    QueueManager register(String topic, BatchMessageHandler messageHandler);

    @Nullable
    QueueManager getQueueManager(String topic);

    ProcessingMessageManager processingMessageManager();

    void startListen() throws SQLException;

    void stopListen();
}
