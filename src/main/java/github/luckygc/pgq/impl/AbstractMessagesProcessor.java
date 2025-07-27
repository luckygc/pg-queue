package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.ProcessingMessageManager;
import github.luckygc.pgq.api.QueueListener;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMessagesProcessor implements QueueListener {

    private static final Logger log = LoggerFactory.getLogger(AbstractMessagesProcessor.class);

    protected final DatabaseQueue queue;
    protected final ProcessingMessageManager processingMessageManager;
    protected final Semaphore semaphore;
    private final ThreadPoolExecutor threadPool;
    private final AtomicInteger threadCount = new AtomicInteger(0);

    public AbstractMessagesProcessor(DatabaseQueue queue, ProcessingMessageManager processingMessageManager,
            int threadCount) {
        this.queue = Objects.requireNonNull(queue);
        this.processingMessageManager = Objects.requireNonNull(processingMessageManager);
        if (threadCount < 1 || threadCount > 200) {
            throw new IllegalArgumentException("线程数必须在1到200之间");
        }

        this.semaphore = new Semaphore(threadCount);
        
        // 创建线程池
        this.threadPool = new ThreadPoolExecutor(
                0, // corePoolSize: 0，空闲时不保留线程
                threadCount, // maximumPoolSize: 最大线程数
                60L, TimeUnit.SECONDS, // keepAliveTime: 线程空闲60秒后回收
                new LinkedBlockingQueue<>(), // workQueue: 无界队列
                new PgqThreadFactory(), // threadFactory: 自定义线程工厂
                new ThreadPoolExecutor.AbortPolicy() // rejectedExecutionHandler: 拒绝策略
        );
        
        // 允许核心线程超时回收
        threadPool.allowCoreThreadTimeOut(true);
    }

    @Override
    public void onMessageAvailable() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            threadPool.execute(() -> {
                try {
                    processMessages();
                } finally {
                    semaphore.release();
                }
            });
        } catch (Throwable t) {
            semaphore.release();
            log.error("提交消息处理任务失败", t);
        }
    }

    /**
     * 关闭线程池，释放资源
     */
    public void shutdown() {
        if (threadPool != null && !threadPool.isShutdown()) {
            threadPool.shutdown();
            try {
                // 等待30秒让任务完成
                if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("线程池未能在30秒内正常关闭，强制关闭");
                    threadPool.shutdownNow();
                    // 再等待30秒
                    if (!threadPool.awaitTermination(30, TimeUnit.SECONDS)) {
                        log.error("线程池强制关闭失败");
                    }
                }
            } catch (InterruptedException e) {
                log.warn("等待线程池关闭时被中断", e);
                threadPool.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 获取线程池状态信息，用于监控
     */
    public String getThreadPoolStatus() {
        if (threadPool == null) {
            return "ThreadPool not initialized";
        }
        return String.format("ThreadPool[Active: %d, Pool: %d, Queue: %d, Completed: %d]",
                threadPool.getActiveCount(),
                threadPool.getPoolSize(),
                threadPool.getQueue().size(),
                threadPool.getCompletedTaskCount());
    }

    public abstract void processMessages();

    /**
     * 自定义线程工厂，用于设置线程名称和属性
     */
    private class PgqThreadFactory implements ThreadFactory {
        
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "pgq-message-processor-%s-%d"
                    .formatted(topic(), threadCount.incrementAndGet()));
            thread.setDaemon(true);
            thread.setUncaughtExceptionHandler((t, e) -> 
                    log.error("线程 {} 发生未捕获异常", t.getName(), e));
            return thread;
        }
    }
}
