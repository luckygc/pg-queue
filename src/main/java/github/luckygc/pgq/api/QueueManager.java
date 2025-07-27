package github.luckygc.pgq.api;

import java.sql.SQLException;
import org.jspecify.annotations.Nullable;

public interface QueueManager {

    DatabaseQueue queue(String topic);

    void registerListener(QueueListener messageListener);

    @Nullable
    QueueListener listener(String topic);

    MessageManager messageManager();

    DeadMessageManger deadMessageManager();

    void start() throws SQLException;

    void stop();
}
