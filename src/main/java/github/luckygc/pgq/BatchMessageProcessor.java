package github.luckygc.pgq;

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
    private final BatchMessageHandler messageHandler;

    public BatchMessageProcessor(BatchMessageHandler messageHandler) {
        this.messageHandler = Objects.requireNonNull(messageHandler);
        if (messageHandler.threadCount() < 1) {
            throw new IllegalArgumentException("threadCount必须大于0");
        }
        this.semaphore = new Semaphore(messageHandler.threadCount());
    }

    @Override
    public void onMessageAvailable(PgQueue queue) {
        if (!semaphore.tryAcquire()) {
            return;
        }

        Runnable r = () -> {
            try {
                List<Message> messages;
                while (!(messages = queue.pull(messageHandler.pullCount())).isEmpty()) {
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
