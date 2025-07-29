package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageProcessor;
import github.luckygc.pgq.api.callback.MessageAvailableCallback;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class MessageProcessorDispatcher implements MessageAvailableCallback {

    private final Map<String, MessageProcessor> processorMap = new ConcurrentHashMap<>();

    @Override
    public void onMessageAvailable(String topic) {
        dispatchProcessor(topic);
    }

    public void register(MessageProcessor messageProcessor) {
        String topic = messageProcessor.topic();
        if (processorMap.putIfAbsent(topic, messageProcessor) != null) {
            throw new IllegalStateException("当前已存在topic[%s]的消息处理器".formatted(topic));
        }
    }

    public void dispatchProcessor(String topic) {
        MessageProcessor messageProcessor = processorMap.get(topic);
        if (messageProcessor == null) {
            return;
        }

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

                @Override
                public void afterCommit() {
                    messageProcessor.process();
                }
            });
        } else {
            messageProcessor.process();
        }
    }
}
