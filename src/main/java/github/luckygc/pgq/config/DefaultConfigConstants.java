package github.luckygc.pgq.config;

import java.time.Duration;

public class DefaultConfigConstants {

    /**
     * 优先级
     */
    public static final int priority = 0;

    /**
     * 首次处理延迟,默认无延迟
     */
    public static final Duration firstProcessDelay = Duration.ZERO;

    /**
     * 最大尝试次数
     */
    public static final int maxAttempt = 1;
    /**
     * 下一次处理延迟,默认一分钟
     */
    public static final Duration nextProcessDelay = Duration.ofMinutes(1);

    /**
     * 轮询间隔,默认一分钟
     */
    public static final Duration pollingInterval = Duration.ofMinutes(1);

    /**
     * 一次拉取消息的数量,默认50
     */
    public static final long pullBatchSize = 50;
}
