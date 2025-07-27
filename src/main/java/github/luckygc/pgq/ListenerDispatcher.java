package github.luckygc.pgq;

import github.luckygc.pgq.api.QueueListener;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class ListenerDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ListenerDispatcher.class);

    private final Map<String, QueueListener> listenerMap = new ConcurrentHashMap<>();

    public void registerListener(QueueListener listener) {
        QueueListener queueListener = listenerMap.putIfAbsent(listener.topic(), listener);
        if (queueListener != null) {
            throw new IllegalStateException("当前已存在topic[{}]的监听器");
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
        QueueListener listener = listenerMap.get(topic);
        if (listener == null) {
            return;
        }

        listener.onMessageAvailable();
    }
}
