package github.luckygc.pgq;

import github.luckygc.pgq.api.callback.MessageAvailableCallback;
import github.luckygc.pgq.impl.MessagesAvailableCallBackImpl;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class MessageAvailableCallbackDispatcher {

    private static final Logger log = LoggerFactory.getLogger(MessageAvailableCallbackDispatcher.class);

    private final Map<String, MessageAvailableCallback> callbackMap = new ConcurrentHashMap<>();

    public void register(String topic, MessageAvailableCallback callback) {
        MessageAvailableCallback messageAvailableCallback = callbackMap.putIfAbsent(topic, callback);
        if (messageAvailableCallback != null) {
            throw new IllegalStateException("当前已存在topic[%s]的消息可用回调".formatted(topic));
        }
    }

    public boolean unregister(String topic, MessageAvailableCallback callback) {
        return callbackMap.remove(topic, callback);
    }

    public void dispatchCallback(String topic) {
        MessageAvailableCallback callback = callbackMap.get(topic);
        if (callback == null) {
            return;
        }

        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

                @Override
                public void afterCommit() {
                    callback.onMessageAvailable();
                }
            });
        } else {
            callback.onMessageAvailable();
        }
    }

    /**
     * 关闭所有消息处理器的线程池
     */
    public void shutdown() {
        for (MessageAvailableCallback listener : callbackMap.values()) {
            if (listener instanceof MessagesAvailableCallBackImpl processor) {
                try {
                    processor.shutdown();
                    log.debug("已关闭消息处理器线程池: topic={}", listener.topic());
                } catch (Exception e) {
                    log.error("关闭消息处理器线程池失败: topic={}", listener.topic(), e);
                }
            }
        }
    }
}
