package github.luckygc.pgq.support;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.MessageManager;
import github.luckygc.pgq.api.MessageProcessor;
import github.luckygc.pgq.api.PgQueue;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;

public abstract class AbstractMessageProcessor implements MessageProcessor {

    private final Semaphore semaphore;
    protected final PgQueue pgQueue;
    protected final MessageManager messageManager;

    public AbstractMessageProcessor(PgQueue pgQueue, MessageManager messageManager) {
        this(pgQueue, messageManager, 1);
    }

    public AbstractMessageProcessor(PgQueue pgQueue, MessageManager messageManager, int threadCount) {
        this.pgQueue = Objects.requireNonNull(pgQueue);
        this.messageManager = Objects.requireNonNull(messageManager);

        if (threadCount < 1) {
            throw new IllegalArgumentException("threadCount必须大于0");
        }
        this.semaphore = new Semaphore(threadCount);

    }

    @Override
    public String topic() {
        return pgQueue.getTopic();
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

    @Override
    public void processMessages() {
        int pullCount = pullCount();
        if (pullCount < 1) {
            throw new IllegalStateException("pullCount必须大于0");
        }

        if (pullCount == 1) {
            pullOneAndHandle(pgQueue);
        } else {
            pullBatchAndHandle(pgQueue);
        }
    }

    private void pullOneAndHandle(PgQueue pgQueue) {
        Message message;
        while ((message = pgQueue.pull()) != null) {
            try {
                handleMessage(message);
            } catch (Exception e) {
                messageManager.dead(message);
            }
        }
    }

    private void pullBatchAndHandle(PgQueue queue) {

    }

    public int pullCount() {
        return 50;
    }

    protected abstract int messageHandleCount();

    protected abstract void handleMessage(Message message);

    protected abstract void handleMessages(List<Message> messages);
}
