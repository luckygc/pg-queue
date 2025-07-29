package github.luckygc.pgq.tool;

import github.luckygc.pgq.dao.QueueDao;
import java.util.List;
import java.util.Objects;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class PgNotifier {

    private final QueueDao queueDao;

    public PgNotifier(QueueDao queueDao) {
        this.queueDao = Objects.requireNonNull(queueDao);
    }

    public void sendNotify(String topic) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    queueDao.sendNotify(topic);
                }
            });
        } else {
            queueDao.sendNotify(topic);
        }
    }

    public void sendNotify(List<String> topics) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    queueDao.sendNotify(topics);
                }
            });
        } else {
            queueDao.sendNotify(topics);
        }
    }
}
