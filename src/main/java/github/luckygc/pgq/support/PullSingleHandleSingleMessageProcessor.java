package github.luckygc.pgq.support;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.MessageHandler;
import github.luckygc.pgq.api.MessageListener;
import github.luckygc.pgq.api.MultiMessageHandler;
import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.SingleMessageHandler;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PullSingleHandleSingleMessageProcessor implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(PullSingleHandleSingleMessageProcessor.class);
    private final AtomicBoolean runningFlag = new AtomicBoolean(false);
    private final PgQueue pgQueue;
    private final boolean isHandleSingle;

    public PullSingleHandleSingleMessageProcessor(PgQueue pgQueue, MessageHandler messageHandler, int pullCount,
            int threadCount) {
        this.pgQueue = Objects.requireNonNull(pgQueue);
        if (messageHandler instanceof SingleMessageHandler handler) {
            singleMessageHandler = handler;
            isHandleSingle = true;
        } else if (messageHandler instanceof MultiMessageHandler handler) {
            multiMessageHandler = handler;
            isHandleSingle = false;
        } else {
            throw new IllegalArgumentException("messageHandler必须是SingleMessageHandler或者MultiMessageHandler");
        }

        if (pullCount < 1) {
            throw new IllegalArgumentException("pullCount必须大于0");
        }
        this.pullCount = pullCount;

        if (pullCount == 1 && !isHandleSingle) {
            throw new IllegalArgumentException("pullCount等于1时messageHandler必须是SingleMessageHandler");
        }

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
        if (!runningFlag.compareAndSet(false, true)) {
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
            log.error("启动处理线程失败", t);
            semaphore.release();
        }
    }

    public void processMessages() {
        if (pullCount == 1) {
            pullOneAndHandle();
        } else {
            pullBatchAndHandle();
        }
    }

    private void pullOneAndHandle() {
        Message message;
        while ((message = pgQueue.pull()) != null) {
            try {
                singleMessageHandler.handle(message);
            } catch (Throwable t) {
                log.error("处理消息失败", t);
            }
        }
    }

    private void pullBatchAndHandle() {
        List<Message> messages;
        while (!(messages = pgQueue.pull(pullCount)).isEmpty()) {
            if (isHandleSingle) {
                for (Message message : messages) {
                    try {
                        singleMessageHandler.handle(message);
                    } catch (Throwable t) {
                        log.error("处理消息失败", t);
                        pgQueue.dead(message);
                    }
                }
            } else {
                try {
                    multiMessageHandler.handle(messages);
                } catch (Throwable t) {
                    log.error("处理消息失败", t);
                    pgQueue.dead(messages);
                }
            }
        }
    }
}
