package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageManager;
import java.time.Duration;
import java.util.List;

public class MessageManagerImpl implements MessageManager {

    private final QueueDao queueDao;

    public MessageManagerImpl(QueueDao queueDao) {
        this.queueDao = queueDao;
    }

    @Override
    public void complete(Message message) {
        queueDao.completeProcessingMessage(message);
    }

    @Override
    public void complete(List<Message> messages) {
        queueDao.completeProcessingMessages(messages);
    }

    @Override
    public void dead(Message message) {
        queueDao.deadProcessingMessage(message);
    }

    @Override
    public void dead(List<Message> messages) {
        queueDao.deadProcessingMessages(messages);
    }

    @Override
    public void delete(Message message) {
        queueDao.deleteProcessingMessage(message);
    }

    @Override
    public void delete(List<Message> messages) {
        queueDao.deleteProcessingMessages(messages);
    }

    @Override
    public void retry(Message message) {
        queueDao.retryProcessingMessage(message);
    }

    @Override
    public void retry(Message message, Duration processDelay) {
        queueDao.retryProcessingMessage(message, processDelay);
    }

    @Override
    public void retry(List<Message> messages) {
        queueDao.retryProcessingMessages(messages);
    }

    @Override
    public void retry(List<Message> messages, Duration processDelay) {
        queueDao.retryProcessingMessages(messages, processDelay);
    }
}
