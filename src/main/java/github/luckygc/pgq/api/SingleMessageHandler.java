package github.luckygc.pgq.api;

import github.luckygc.pgq.PgqConstants;
import github.luckygc.pgq.model.Message;

public interface SingleMessageHandler {

    default int pullCount() {
        return PgqConstants.MESSAGE_HANDLER_PULL_COUNT;
    }

    default int threadCount() {
        return PgqConstants.MESSAGE_HANDLER_THREAD_COUNT;
    }

    String topic();

    void handle(ProcessingMessageManager processingMessageManager, Message message);
}
