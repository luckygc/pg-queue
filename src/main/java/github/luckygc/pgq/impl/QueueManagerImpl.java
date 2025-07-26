package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.QueueListener;
import github.luckygc.pgq.api.QueueManager;
import org.jspecify.annotations.Nullable;

public record QueueManagerImpl(PgQueue pgQueue, @Nullable QueueListener messageListener) implements QueueManager {

    @Override
    public PgQueue queue() {
        return pgQueue;
    }

    @Override
    public @Nullable QueueListener messageListener() {
        return messageListener;
    }
}
