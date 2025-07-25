package github.luckygc.pgq.api;

import org.jspecify.annotations.Nullable;

public interface PgqManager {

    void registerQueue(PgQueue queue);

    @Nullable
    PgQueue getQueue(String topic);

    void registerMessageProcessor();

    @Nullable
    MessageProcessor getMessageProcessor(String topic);

    MessageManager messageManager();
}
