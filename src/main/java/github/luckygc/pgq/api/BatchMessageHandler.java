package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.PgqConstants;
import java.util.List;

public interface BatchMessageHandler {

    default int pullCount() {
        return PgqConstants.MESSAGE_HANDLER_PULL_COUNT;
    }

    default int threadCount() {
        return PgqConstants.MESSAGE_HANDLER_THREAD_COUNT;
    }

    String topic();

    void handle(ProcessingMessageManager processingMessageManager, List<Message> messages);
}
