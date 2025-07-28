package github.luckygc.pgq.api.manager;

import github.luckygc.pgq.api.MessageQueue;

public interface PgmqManager {

    MessageQueue queue();

    MessageManager messageManager();

    HandlerManager handler();
}
