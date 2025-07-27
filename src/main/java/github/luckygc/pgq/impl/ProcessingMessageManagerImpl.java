package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.ProcessingMessageManager;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;

public class ProcessingMessageManagerImpl implements ProcessingMessageManager {

    private final MessageDao messageDao;

    public ProcessingMessageManagerImpl(MessageDao messageDao) {
        this.messageDao = messageDao;
    }

    @Override
    public void complete(Message message) {
        messageDao.completeProcessingMessage(message);
    }

    @Override
    public void complete(List<Message> messages) {
        messageDao.completeProcessingMessages(messages);
    }

    @Override
    public void dead(Message message) {
        messageDao.deadProcessingMessage(message);
    }

    @Override
    public void dead(List<Message> messages) {
        messageDao.deadProcessingMessages(messages);
    }

    @Override
    public void delete(Message message) {
        messageDao.deleteProcessingMessage(message);
    }

    @Override
    public void delete(List<Message> messages) {
        messageDao.deleteProcessingMessages(messages);
    }

    @Override
    public void retry(Message message) {
        messageDao.retryProcessingMessage(message);
    }

    @Override
    public void retry(Message message, Duration processDelay) {
        messageDao.retryProcessingMessage(message, processDelay);
    }

    @Override
    public void retry(List<Message> messages) {
        messageDao.retryProcessingMessages(messages);
    }

    @Override
    public void retry(List<Message> messages, Duration processDelay) {
        messageDao.retryProcessingMessages(messages, processDelay);
    }
}
