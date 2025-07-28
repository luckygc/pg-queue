package github.luckygc.pgq.impl;

import github.luckygc.pgq.Utils;
import github.luckygc.pgq.api.manager.MessageManager;
import github.luckygc.pgq.dao.MessageDao;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

public class MessageManagerImpl implements MessageManager {

    private final MessageDao messageDao;

    public MessageManagerImpl(MessageDao messageDao) {
        this.messageDao = messageDao;
    }


    @Override
    public void dead(Long id) {
        messageDao.moveProcessingMsgToDeadById(id);
    }

    @Override
    public void delete(Long id) {
        messageDao.deleteProcessingMsgById(id);
    }

    @Override
    public void retry(Long id) {
        messageDao.moveProcessingMsgToPendingById(id);
    }

    @Override
    public void retry(Long id, Duration processDelay) {
        Objects.requireNonNull(processDelay);
        Utils.checkDurationIsPositive(processDelay);

        LocalDateTime visibleTime = LocalDateTime.now().plus(processDelay);
        messageDao.moveProcessingMsgToInvisibleById(id, visibleTime);
    }
}
