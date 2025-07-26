package github.luckygc.pgq.api;

import java.sql.SQLException;
import org.jspecify.annotations.Nullable;

public interface PgqManager {

    PgQueue registerQueue(String topic);

    PgQueue registerQueue(String topic, SingleMessageHandler messageHandler);

    PgQueue registerQueue(String topic, BatchMessageHandler messageHandler);

    @Nullable
    PgQueue getQueue(String topic);

    void startListen() throws SQLException;

    void stopListen();
}
