package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.MessageManager;
import github.luckygc.pgq.api.QueueListener;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessagesProcessor implements QueueListener {

    private static final Logger log = LoggerFactory.getLogger(AbstractMessagesProcessor.class);

    protected final DatabaseQueue queue;
    protected final MessageManager messageManager;
    protected final Semaphore semaphore;

    public AbstractMessagesProcessor(DatabaseQueue queue, MessageManager messageManager, Semaphore semaphore) {
        this.queue = Objects.requireNonNull(queue);
        this.messageManager = Objects.requireNonNull(messageManager);
        this.semaphore = Objects.requireNonNull(semaphore);
        if (semaphore.availablePermits() < 1) {
            throw new IllegalArgumentException("线程数必须大于0");
        }
    }

    @Override
    public void onMessageAvailable() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            Thread thread = new Thread(this::processMessages);
            thread.setDaemon(true);
            thread.start();
        } catch (Throwable t) {
            semaphore.release();
            log.error("启动消息处理线程失败", t);
        }
    }

    public abstract void processMessages();
}
