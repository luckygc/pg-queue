package github.luckygc.pgq.api;

import github.luckygc.pgq.api.handler.MessageHandler;

public interface PgmqManager {

    MessageQueue queue();

    DelayMessageQueue delayQueue();

    PriorityMessageQueue priorityQueue();

    void registerHandler(MessageHandler messageHandler);

    void unregisterHandler(MessageHandler messageHandler);

    void shutdown();
}
