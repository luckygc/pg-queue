package github.luckygc.pgq.api;

import org.jspecify.annotations.Nullable;

public interface PgqManager {

    PgQueue registerQueue(String topic);

    @Nullable
    PgQueue getQueue(String topic);

    void registerMessageProcessor(String topic, MessageListener messageListener);

    @Nullable
    MessageListener getMessageProcessor(String topic);

    ProcessingMessageManager messageManager();
}
