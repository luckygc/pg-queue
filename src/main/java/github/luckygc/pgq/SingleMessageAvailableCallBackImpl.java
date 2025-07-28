package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.manager.MessageManager;
import github.luckygc.pgq.api.handler.SingleMessageHandler;
import github.luckygc.pgq.impl.MessagesAvailableCallBackImpl;
import github.luckygc.pgq.model.MessageDO;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleMessageAvailableCallBackImpl extends MessagesAvailableCallBackImpl {

    private static final Logger log = LoggerFactory.getLogger(SingleMessageAvailableCallBackImpl.class);

    private final SingleMessageHandler messageHandler;

    public SingleMessageAvailableCallBackImpl(MessageQueue queue, MessageManager messageManager,
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
        List<MessageDO> messageDOS;
        int pullCount = messageHandler.pullCount();
        if (pullCount < 1 || pullCount > 5000) {
            throw new IllegalStateException("pullCount必须在1到5000之间");
        }
        while (!(messageDOS = queue.pull(pullCount)).isEmpty()) {
            for (MessageDO messageDO : messageDOS) {
                try {
                    messageHandler.handle(messageManager, messageDO);
                } catch (Throwable t) {
                    log.error("处理消息失败", t);
                }
            }
        }
    }
}
