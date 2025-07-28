package github.luckygc.pgq;

import github.luckygc.pgq.api.callback.MessageAvailabilityCallback;
import github.luckygc.pgq.impl.AbstractMessagesProcessor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class ListenerDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ListenerDispatcher.class);

    private final Map<String, MessageAvailabilityCallback> listenerMap = new ConcurrentHashMap<>();

    public void registerListener(MessageAvailabilityCallback listener) {
        MessageAvailabilityCallback messageAvailabilityCallback = listenerMap.putIfAbsent(listener.topic(), listener);
        if (messageAvailabilityCallback != null) {
            throw new IllegalStateException("当前已存在topic[%s]的监听器".formatted(listener.topic()));
        }
    }

    public void dispatchWithCheckTx(String topic) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {

                @Override
                public void afterCommit() {
                    dispatch(topic);
                }
            });
        } else {
            dispatch(topic);
        }
    }

    public void dispatch(String topic) {
        MessageAvailabilityCallback listener = listenerMap.get(topic);
        if (listener == null) {
            return;
        }

        listener.onMessageAvailable();
    }

    /**
     * 关闭所有消息处理器的线程池
     */
    public void shutdown() {
        for (MessageAvailabilityCallback listener : listenerMap.values()) {
            if (listener instanceof AbstractMessagesProcessor processor) {
                try {
                    processor.shutdown();
                    log.debug("已关闭消息处理器线程池: topic={}", listener.topic());
                } catch (Exception e) {
                    log.error("关闭消息处理器线程池失败: topic={}", listener.topic(), e);
                }
            }
        }
    }

    /**
     * 获取所有消息处理器的线程池状态，用于监控
     */
    public Map<String, String> getThreadPoolStatus() {
        Map<String, String> statusMap = new ConcurrentHashMap<>();
        for (Map.Entry<String, MessageAvailabilityCallback> entry : listenerMap.entrySet()) {
            if (entry.getValue() instanceof AbstractMessagesProcessor processor) {
                statusMap.put(entry.getKey(), processor.getThreadPoolStatus());
            }
        }
        return statusMap;
    }
}
