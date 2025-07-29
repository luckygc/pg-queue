package github.luckygc.pgq.api;

import github.luckygc.pgq.api.handler.MessageHandler;

public interface PgmqManager {

    MessageQueue queue();

    void registerHandler(MessageHandler messageHandler);
}
