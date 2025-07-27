package github.luckygc.pgq.impl;

import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.api.BatchMessageHandler;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.ProcessingMessageManager;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMessageProcessor extends AbstractMessagesProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchMessageProcessor.class);

    private final BatchMessageHandler messageHandler;

    public BatchMessageProcessor(DatabaseQueue queue, ProcessingMessageManager processingMessageManager,
            BatchMessageHandler messageHandler) {
        super(queue, processingMessageManager, new Semaphore(messageHandler.threadCount()));
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
            while (!(messages = queue.pull(messageHandler.pullCount())).isEmpty()) {
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
