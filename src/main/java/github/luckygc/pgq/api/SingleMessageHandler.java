package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.PgqConstants;

@FunctionalInterface
public interface SingleMessageHandler {

    default int pullCount() {
        return PgqConstants.MESSAGE_HANDLER_PULL_COUNT;
    }

    default int threadCount() {
        return PgqConstants.MESSAGE_HANDLER_THREAD_COUNT;
    }

    void handle(Message message);
}
