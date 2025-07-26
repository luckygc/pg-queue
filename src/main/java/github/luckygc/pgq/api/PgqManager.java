package github.luckygc.pgq.api;

import github.luckygc.pgq.QueueDao;
import java.sql.SQLException;
import org.jspecify.annotations.Nullable;

public interface PgqManager {

    PgQueue registerQueue(String topic);

    @Nullable
    PgQueue getQueue(String topic);

    void startListen() throws SQLException;

    void stopListen();
}
