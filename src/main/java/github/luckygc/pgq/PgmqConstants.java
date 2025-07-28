package github.luckygc.pgq;

import java.time.Duration;

public class PgmqConstants {

    /**
     * 默认消息优先级
     */
    public static final int MESSAGE_PRIORITY = 0;

    /**
     * 默认消息处理超时时间
     */
    public static final Duration PROCESS_TIMEOUT = Duration.ofMinutes(10);

    public static final int MESSAGE_HANDLER_PULL_COUNT = 50;
    public static final int MESSAGE_HANDLER_THREAD_COUNT = 1;

    public static final int PGQ_ID = 199738;
    public static final int SCHEDULER_ID = 1;
    public static final String TOPIC_CHANNEL = "pgq_topic_channel";
}
