package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageListener;
import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.SingleMessageHandler;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleMessageProcessor implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(SingleMessageProcessor.class);
    private final Semaphore semaphore;
    private final PgQueue pgQueue;
    private final SingleMessageHandler messageHandler;

    public SingleMessageProcessor(PgQueue pgQueue, SingleMessageHandler messageHandler) {
        this.pgQueue = Objects.requireNonNull(pgQueue);
        this.messageHandler = Objects.requireNonNull(messageHandler);

        if (messageHandler.threadCount() < 1) {
            throw new IllegalArgumentException("threadCount必须大于0");
        }
        this.semaphore = new Semaphore(messageHandler.threadCount());
    }

    @Override
    public void onMessageAvailable() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        Runnable r = () -> {
            try {
                List<Message> messages;
                while (!(messages = pgQueue.pull(messageHandler.pullCount())).isEmpty()) {
                    for (Message message : messages) {
                        try {
                            messageHandler.handle(message);
                        } catch (Throwable t) {
                            log.error("处理消息失败", t);
                        }
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
