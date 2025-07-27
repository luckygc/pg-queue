package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.DeadMessageManger;
import github.luckygc.pgq.dao.MessageDao;

public class DeadMessageManagerImpl implements DeadMessageManger {

    private final MessageDao messageDao;

    public DeadMessageManagerImpl(MessageDao messageDao) {
        this.messageDao = messageDao;
    }

    @Override
    public void retry(String topic) {
        throw new UnsupportedOperationException("暂未实现");
    }

    @Override
    public void delete(String topic) {
        throw new UnsupportedOperationException("暂未实现");
    }
}
