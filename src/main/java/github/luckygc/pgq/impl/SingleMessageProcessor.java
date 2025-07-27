package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.ProcessingMessageManager;
import github.luckygc.pgq.api.SingleMessageHandler;
import github.luckygc.pgq.model.Message;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleMessageProcessor extends AbstractMessagesProcessor {

    private static final Logger log = LoggerFactory.getLogger(SingleMessageProcessor.class);

    private final SingleMessageHandler messageHandler;

    public SingleMessageProcessor(DatabaseQueue queue, ProcessingMessageManager processingMessageManager,
            SingleMessageHandler messageHandler) {
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
                for (Message message : messages) {
                    try {
                        messageHandler.handle(processingMessageManager, message);
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
