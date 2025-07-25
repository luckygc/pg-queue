package github.luckygc.pgq.api;

import org.jspecify.annotations.Nullable;

public interface PgqManager {

    PgQueue registerQueue(String topic);

    @Nullable
    PgQueue getQueue(String topic);

    void registerMessageProcessor(String topic, MessageProcessor messageProcessor);

    @Nullable
    MessageProcessor getMessageProcessor(String topic);

    MessageManager messageManager();
}
