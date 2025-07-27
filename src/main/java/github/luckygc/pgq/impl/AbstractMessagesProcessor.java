package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.ProcessingMessageManager;
import github.luckygc.pgq.api.QueueListener;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessagesProcessor implements QueueListener {

    private static final Logger log = LoggerFactory.getLogger(AbstractMessagesProcessor.class);

    protected final DatabaseQueue queue;
    protected final ProcessingMessageManager processingMessageManager;
    protected final Semaphore semaphore;
    private final AtomicInteger threadCount = new AtomicInteger(0);

    public AbstractMessagesProcessor(DatabaseQueue queue, ProcessingMessageManager processingMessageManager,
            int threadCount) {
        this.queue = Objects.requireNonNull(queue);
        this.processingMessageManager = Objects.requireNonNull(processingMessageManager);
        if (threadCount < 1 || threadCount > 200) {
            throw new IllegalArgumentException("线程数必须在1到200之间");
        }

        this.semaphore = new Semaphore(threadCount);
    }

    @Override
    public void onMessageAvailable() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            Thread thread = new Thread(this::processMessages,
                    "pgq-message-processor-%d".formatted(threadCount.incrementAndGet()));
            thread.setDaemon(true);
            thread.start();
        } catch (Throwable t) {
            semaphore.release();
            log.error("启动消息处理线程失败", t);
        }
    }

    public abstract void processMessages();
}
