package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.BatchMessageHandler;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.ProcessingMessageManager;
import github.luckygc.pgq.model.Message;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMessageProcessor extends AbstractMessagesProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchMessageProcessor.class);

    private final BatchMessageHandler messageHandler;

    public BatchMessageProcessor(DatabaseQueue queue, ProcessingMessageManager processingMessageManager,
            BatchMessageHandler messageHandler) {
        super(queue, processingMessageManager, messageHandler.threadCount());
        this.messageHandler = Objects.requireNonNull(messageHandler);
    }

    @Override
    public String topic() {
        return messageHandler.topic();
    }

    @Override
    public void processMessages() {
        try {
            List<Message> messages;
            int pullCount = messageHandler.pullCount();
            if (pullCount < 1 || pullCount > 5000) {
                throw new IllegalStateException("pullCount必须在1到5000之间");
            }
            while (!(messages = queue.pull(pullCount)).isEmpty()) {
                try {
                    messageHandler.handle(processingMessageManager, messages);
                } catch (Throwable t) {
                    log.error("处理消息失败", t);
                }
            }
        } finally {
            semaphore.release();
        }
    }
}
