package github.luckygc.pgq;

import java.time.Duration;

public class PgqConstants {

    /**
     * 默认消息优先级
     */
    public static final int MESSAGE_PRIORITY = 0;

    /**
     * 默认消息处理超时时间
     */
    public static final Duration PROCESS_TIMEOUT = Duration.ofMinutes(10);

    /*--------------------------------sql start----------------------------------*/
    private static final int PGQ_ID = 199738;
    private static final int SCHEDULER_ID = 1;
    private static final String CHANNEL_NAME = "pgq_topic_channel";
    /*--------------------------------sql end------------------------------------*/
}
