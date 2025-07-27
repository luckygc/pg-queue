package github.luckygc.pgq.impl;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.BatchMessageHandler;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.QueueManager;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMessageQueueListener extends AbstractMessagesProcessor {

    private static final Logger log = LoggerFactory.getLogger(BatchMessageQueueListener.class);

    private final BatchMessageHandler messageHandler;

    public BatchMessageQueueListener(QueueManager queueManager, BatchMessageHandler messageHandler) {
        super(queueManager, new Semaphore(messageHandler.threadCount()));
        this.messageHandler = Objects.requireNonNull(messageHandler);
    }

    @Override
    public String topic() {
        return messageHandler.topic();
    }

    @Override
    public void processMessages() {
        try {
            DatabaseQueue queue = queueManager.queue(messageHandler.topic());
            List<Message> messages;
            while (!(messages = queue.pull(messageHandler.pullCount())).isEmpty()) {
                try {
                    messageHandler.handle(messages);
                } catch (Throwable t) {
                    log.error("处理消息失败", t);
                }
            }
        } finally {
            semaphore.release();
        }
    }
}
