package github.luckygc.pgq;

import github.luckygc.pgq.config.QueueConfig;

public interface PgQueue<M> {

    QueueConfig getConfig();

    void push(M message);
}
