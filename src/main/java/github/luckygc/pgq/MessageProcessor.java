package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.api.manager.MessageManager;
import github.luckygc.pgq.model.Message;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private MessageQueue messageQueue;
    private MessageManager messageManager;
    private final MessageHandler messageHandler;
    private final String topic;
    private final int maxPoll;
    private final Semaphore semaphore;
    private final ThreadPoolExecutor threadPool;

    public MessageProcessor(MessageQueue messageQueue, MessageHandler messageHandler) {
        this.messageHandler = Objects.requireNonNull(messageHandler);
        this.topic = Objects.requireNonNull(messageHandler.topic());

        this.maxPoll = messageHandler.maxPoll();
        if (maxPoll < 1 || maxPoll > 5000) {
            throw new IllegalArgumentException("maxPoll必须在1-5000之间");
        }

        int threadCount = messageHandler.threadCount();
        if (threadCount < 1 || threadCount > 200) {
            throw new IllegalArgumentException("threadCount必须在1-200之间");
        }

        this.semaphore = new Semaphore(threadCount);
        // 创建线程池
        this.threadPool = new ThreadPoolExecutor(
                0, // corePoolSize: 0，空闲时不保留线程
                threadCount, // maximumPoolSize: 最大线程数
                60L, TimeUnit.SECONDS, // keepAliveTime: 线程空闲60秒后回收
                new ArrayBlockingQueue<>(threadCount),
                new PgmqHandlerThreadFactory(topic),
                new ThreadPoolExecutor.AbortPolicy() // rejectedExecutionHandler: 拒绝策略
        );

        // 允许核心线程超时回收
        threadPool.allowCoreThreadTimeOut(true);
    }

    public void asyncProcess() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            threadPool.execute(this::loopPollAndHandle);
        } catch (Throwable t) {
            semaphore.release();
            log.error("提交消息处理任务失败", t);
        }
    }

    public void loopPollAndHandle() {
        try {
            List<Message> messages;
            while (!(messages = messageQueue.poll(topic, maxPoll)).isEmpty()) {
                for (Message message : messages) {
                    try {
                        messageHandler.handle(message);
                    } catch (Throwable t) {
                        log.error("处理消息失败", t);
                    }
                }
            }
        } catch (Throwable t) {
            log.error("拉取消息失败", t);
        } finally {
            semaphore.release();
        }
    }

    public void shutdown() {
        threadPool.shutdownNow();
    }
}
