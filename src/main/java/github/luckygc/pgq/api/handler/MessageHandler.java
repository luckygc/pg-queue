package github.luckygc.pgq.api.handler;

import github.luckygc.pgq.model.PgmqConstants;
import github.luckygc.pgq.model.Message;

public interface MessageHandler {

    /**
     * 范围[1,5000]
     */
    default int maxPoll() {
        return PgmqConstants.MAX_POLL;
    }

    /**
     * 范围[1,200]
     */
    default int threadCount() {
        return PgmqConstants.MESSAGE_HANDLER_THREAD_COUNT;
    }

    String topic();

    void handle(Message message);
}
