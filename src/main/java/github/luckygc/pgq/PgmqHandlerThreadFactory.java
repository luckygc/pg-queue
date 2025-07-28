package github.luckygc.pgq;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgmqHandlerThreadFactory implements ThreadFactory {

    private static final Logger log = LoggerFactory.getLogger(PgmqHandlerThreadFactory.class);

    private final AtomicInteger threadCount = new AtomicInteger(0);
    private final String topic;

    public PgmqHandlerThreadFactory(String topic) {
        this.topic = Objects.requireNonNull(topic);
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "pgmq-handler-%s-%d".formatted(topic, threadCount.incrementAndGet()));
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((t, e) -> log.error("线程 {} 发生未捕获异常", t.getName(), e));
        return thread;
    }
}
