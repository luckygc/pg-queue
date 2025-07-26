package github.luckygc.pgq.api;

import github.luckygc.pgq.QueueDao;
import org.jspecify.annotations.Nullable;

public interface PgqManager {

    PgQueue registerQueue(String topic);

    @Nullable
    PgQueue getQueue(String topic);

    QueueDao queueDao();

    void startListen();

    void stopListen();
}
