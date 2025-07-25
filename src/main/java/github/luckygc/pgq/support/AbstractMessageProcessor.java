package github.luckygc.pgq.support;

import github.luckygc.pgq.api.MessageProcessor;
import github.luckygc.pgq.api.PgQueue;
import java.util.concurrent.Semaphore;

public abstract class AbstractMessageProcessor implements MessageProcessor {

    private final PgQueue pgQueue;
    private final Semaphore semaphore;

    public AbstractMessageProcessor(PgQueue pgQueue) {
        this(pgQueue, 1);
    }

    public AbstractMessageProcessor(PgQueue pgQueue, int threadCount) {
        this.pgQueue = pgQueue;
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount必须大于0");
        }
        this.semaphore = new Semaphore(threadCount);
    }

    @Override
    public void onMessageAvailable() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            Thread thread = new Thread(() -> {
                try {
                    processMessages();
                } finally {
                    semaphore.release();
                }
            });
            thread.setDaemon(true);
            thread.start();
        } catch (Throwable t) {
            semaphore.release();
        }
    }

    protected abstract void processMessages();
}
