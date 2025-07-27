package github.luckygc.pgq;

import github.luckygc.pgq.api.QueueListener;
import github.luckygc.pgq.api.QueueManager;
import java.time.Duration;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class ListenerDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ListenerDispatcher.class);

    private final QueueManager queueManager;
    private static final long DISPATCH_TIMEOUT_MILLIS = Duration.ofMinutes(1).toMillis();

    public ListenerDispatcher(QueueManager queueManager) {
        this.queueManager = Objects.requireNonNull(queueManager);
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
        QueueListener listener = queueManager.listener(topic);
        if (listener == null) {
            return;
        }

        long start = System.currentTimeMillis();
        listener.onMessageAvailable();
        long end = System.currentTimeMillis();
        if ((end - start) > DISPATCH_TIMEOUT_MILLIS) {
            log.warn("onMessageAvailable方法执行时间过长,请不要阻塞调用, topic:{}", topic);
        }
    }
}
