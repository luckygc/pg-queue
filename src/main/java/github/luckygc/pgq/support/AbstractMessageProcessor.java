package github.luckygc.pgq.support;

import github.luckygc.pgq.api.MessageProcessor;
import github.luckygc.pgq.api.PgQueue;
import java.util.concurrent.Semaphore;

public abstract class AbstractMessageProcessor implements MessageProcessor {

    private final Semaphore semaphore;

    AbstractMessageProcessor() {
        this.semaphore = new Semaphore(1);
    }

    AbstractMessageProcessor(int threadCount) {
        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount必须大于0");
        }
        this.semaphore = new Semaphore(threadCount);
    }

    @Override
    public void onMessageAvailable(PgQueue pgQueue) {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            Thread thread = new Thread(() -> {
                try {
                    processMessages(pgQueue);
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

    abstract void processMessages(PgQueue pgQueue);
}
