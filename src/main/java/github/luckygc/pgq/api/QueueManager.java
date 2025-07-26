package github.luckygc.pgq.api;

import org.jspecify.annotations.Nullable;

public interface QueueManager {

    PgQueue queue();

    @Nullable
    QueueListener messageListener();
}
