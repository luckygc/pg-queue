package github.luckygc.pgq.api;

import github.luckygc.pgq.config.QueueConfig;
import java.util.List;

public interface PgQueue<M> {

    QueueConfig getConfig();

    void push(M message);

    void push(M message, int priority);

    void push(List<M> messages);

    void push(List<M> messages, int priority);

    void tryStartPollingAsync();

    long deleteCompleted();

    long deleteDead();
}
