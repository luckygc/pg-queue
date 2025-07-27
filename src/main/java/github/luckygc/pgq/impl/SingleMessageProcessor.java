package github.luckygc.pgq.impl;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.QueueManager;
import github.luckygc.pgq.api.SingleMessageHandler;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleMessageProcessor extends AbstractMessagesProcessor {

    private static final Logger log = LoggerFactory.getLogger(SingleMessageProcessor.class);

    private final SingleMessageHandler messageHandler;

    public SingleMessageProcessor(QueueManager queueManager, SingleMessageHandler messageHandler) {
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
                for (Message message : messages) {
                    try {
                        messageHandler.handle(queueManager.messageManager(), message);
                    } catch (Throwable t) {
                        log.error("处理消息失败", t);
                    }
                }
            }
        } finally {
            semaphore.release();
        }
    }
}
