package github.luckygc.pgq.api;

import org.jspecify.annotations.Nullable;

public interface QueueManager {

    DatabaseQueue queue();

    @Nullable
    QueueListener messageListener();
}
