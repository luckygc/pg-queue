package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.manager.MessageManager;
import github.luckygc.pgq.api.handler.SingleMessageHandler;
import github.luckygc.pgq.model.Message;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleMessageProcessor extends AbstractMessagesProcessor {

    private static final Logger log = LoggerFactory.getLogger(SingleMessageProcessor.class);

    private final SingleMessageHandler messageHandler;

    public SingleMessageProcessor(MessageQueue queue, MessageManager messageManager,
            SingleMessageHandler messageHandler) {
        super(queue, messageManager, messageHandler.threadCount());
        this.messageHandler = Objects.requireNonNull(messageHandler);
    }

    @Override
    public String topic() {
        return messageHandler.topic();
    }

    @Override
    public void processMessages() {
        List<Message> messages;
        int pullCount = messageHandler.pullCount();
        if (pullCount < 1 || pullCount > 5000) {
            throw new IllegalStateException("pullCount必须在1到5000之间");
        }
        while (!(messages = queue.pull(pullCount)).isEmpty()) {
            for (Message message : messages) {
                try {
                    messageHandler.handle(messageManager, message);
                } catch (Throwable t) {
                    log.error("处理消息失败", t);
                }
            }
        }
    }
}
