package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.QueueListener;
import github.luckygc.pgq.api.QueueManager;
import org.jspecify.annotations.Nullable;

public record QueueManagerImpl(DatabaseQueue databaseQueue, @Nullable QueueListener messageListener) implements QueueManager {

    @Override
    public DatabaseQueue queue() {
        return databaseQueue;
    }

    @Override
    public @Nullable QueueListener messageListener() {
        return messageListener;
    }
}
