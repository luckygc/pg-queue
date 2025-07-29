package github.luckygc.pgq.tool;

import github.luckygc.pgq.api.MessageProcessor;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class MessageProcessorDispatcher {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessorDispatcher.class);

    private final Map<String, MessageProcessor> processorMap = new ConcurrentHashMap<>();

    public void register(MessageProcessor messageProcessor) {
        String topic = messageProcessor.topic();
        if (processorMap.putIfAbsent(topic, messageProcessor) != null) {
            throw new IllegalStateException("当前已存在topic[%s]的消息处理器".formatted(topic));
        }
    }

    public void dispatch(String topic) {
        Objects.requireNonNull(topic);

        MessageProcessor messageProcessor = processorMap.get(topic);
        if (messageProcessor == null) {
            return;
        }

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

                @Override
                public void afterCommit() {
                    tryProcess(messageProcessor);
                }
            });
        } else {
            tryProcess(messageProcessor);
        }
    }

    public void shutdown() {
        for (MessageProcessor processor : processorMap.values()) {
            try {
                processor.shutdown();
            } catch (Throwable t) {
                log.error("关闭消息处理器失败", t);
            }
        }
    }

    private void tryProcess(MessageProcessor processor) {
        try {
            processor.asyncProcess();
        } catch (Throwable t) {
            log.error("调度消息处理器失败", t);
        }
    }
}
