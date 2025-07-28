package github.luckygc.pgq.api.handler;

import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.api.manager.MessageManager;
import github.luckygc.pgq.model.Message;

public interface SingleMessageHandler {

    /**
     * 范围[1,5000]
     */
    default int pullCount() {
        return PgmqConstants.MESSAGE_HANDLER_PULL_COUNT;
    }

    /**
     * 范围[1,200]
     */
    default int threadCount() {
        return PgmqConstants.MESSAGE_HANDLER_THREAD_COUNT;
    }

    String topic();

    void handle(MessageManager messageManager, Message message);
}
