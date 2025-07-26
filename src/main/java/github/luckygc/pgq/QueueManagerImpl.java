package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageListener;
import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.QueueManager;
import org.jspecify.annotations.Nullable;

public record QueueManagerImpl(PgQueue pgQueue, @Nullable MessageListener messageListener) implements QueueManager {

    @Override
    public PgQueue pgQueue() {
        return pgQueue;
    }

    @Override
    public @Nullable MessageListener messageListener() {
        return messageListener;
    }
}
