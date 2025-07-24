package github.luckygc.pgq;

import github.luckygc.pgq.api.PgQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageProcessor {

    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private final AtomicBoolean runningFlag = new AtomicBoolean(false);

    protected final PgQueue pgQueue;

    public MessageProcessor(PgQueue pgQueue) {
        this.pgQueue = pgQueue;
    }

    public void start() {
        if (!runningFlag.compareAndSet(false, true)) {
            return;
        }

        Runnable runnable = () -> {
            try {
                startInternal();
            } finally {
                runningFlag.set(false);
            }
        };
        Thread thread = new Thread(runnable, "pgq-%s_%d".formatted(pgQueue.getTopic(), threadNumber.incrementAndGet()));
        thread.setDaemon(true);
        thread.start();
    }

    private void startInternal() {

    }
}
