package github.luckygc.pgq.support;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.BatchMessageHandler;
import github.luckygc.pgq.api.MessageListener;
import github.luckygc.pgq.api.PgQueue;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchMessageProcessor implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(BatchMessageProcessor.class);
    private final Semaphore semaphore;
    private final int pullCount;
    private final PgQueue pgQueue;
    private final BatchMessageHandler messageHandler;

    public BatchMessageProcessor(PgQueue pgQueue, BatchMessageHandler messageHandler, int pullCount,
            int threadCount) {
        this.pgQueue = Objects.requireNonNull(pgQueue);
        this.messageHandler = Objects.requireNonNull(messageHandler);
        if (pullCount < 1) {
            throw new IllegalArgumentException("pullCount必须大于0");
        }
        this.pullCount = pullCount;

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

        Runnable r = () -> {
            try {
                List<Message> messages;
                while (!(messages = pgQueue.pull(pullCount)).isEmpty()) {
                    try {
                        messageHandler.handle(messages);
                    } catch (Throwable t) {
                        log.error("处理消息失败", t);
                    }
                }
            } finally {
                semaphore.release();
            }
        };

        try {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.start();
        } catch (Throwable t) {
            semaphore.release();
            log.error("启动消息处理线程失败", t);
        }
    }
}
