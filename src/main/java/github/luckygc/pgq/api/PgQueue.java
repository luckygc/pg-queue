package github.luckygc.pgq.api;

import github.luckygc.pgq.config.QueueConfig;

public interface PgQueue<M> {

    QueueConfig getConfig();

    void push(M message);

    void push(M message, int priority);

    void tryStartPollingAsync();

    long deleteCompleted();
}
