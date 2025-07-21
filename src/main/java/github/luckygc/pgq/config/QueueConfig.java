package github.luckygc.pgq.config;

import java.time.Duration;
import org.springframework.util.StringUtils;

/**
 * 队列配置
 */
public class QueueConfig {

    /**
     * 主题
     */
    private final String topic;
    /**
     * 最大尝试次数,默认1
     */
    private int maxAttempt = 1;
    /**
     * 首次处理延迟,默认无延迟
     */
    private Duration firstProcessDelay = Duration.ZERO;
    /**
     * 下一次处理延迟,默认一分钟
     */
    private Duration nextProcessDelay = Duration.ofMinutes(1);

    /**
     * 轮询间隔,默认一分钟
     */
    private Duration pollingInterval = Duration.ofMinutes(1);

    /**
     * 一次拉取消息的数量,默认50
     */
    private long pullBatchSize = 50;

    private QueueConfig(Builder builder) {
        if (!StringUtils.hasText(builder.topic)) {
            throw new IllegalArgumentException("topic不能为空");
        }

        topic = builder.topic;

        if (builder.maxAttempt != null) {
            maxAttempt = builder.maxAttempt;
        }

        if (maxAttempt < 1) {
            throw new IllegalArgumentException("maxAttempt不能小于1");
        }

        if (builder.firstProcessDelay != null) {
            firstProcessDelay = builder.firstProcessDelay;
        }

        if (builder.nexProcessDelay != null) {
            nextProcessDelay = builder.nexProcessDelay;
        }

        if (builder.pollingInterval != null) {
            pollingInterval = builder.pollingInterval;
        }

        if (builder.pullBatchSize != null) {
            pullBatchSize = builder.pullBatchSize;
        }

        if (pullBatchSize < 1) {
            throw new IllegalArgumentException("pullBatchSize不能小于1");
        }
    }

    public String getTopic() {
        return topic;
    }

    public int getMaxAttempt() {
        return maxAttempt;
    }

    public Duration getFirstProcessDelay() {
        return firstProcessDelay;
    }

    public Duration getNextProcessDelay() {
        return nextProcessDelay;
    }

    public long getPullBatchSize() {
        return pullBatchSize;
    }

    public Duration getPollingInterval() {
        return pollingInterval;
    }

    public static class Builder {

        private String topic;
        private Integer maxAttempt;
        private Duration firstProcessDelay;
        private Duration nexProcessDelay;
        private Duration pollingInterval;
        private Long pullBatchSize;

        public Builder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder maxAttempt(Integer maxAttempt) {
            this.maxAttempt = maxAttempt;
            return this;
        }

        public Builder firstProcessDelay(Duration firstProcessDelay) {
            this.firstProcessDelay = firstProcessDelay;
            return this;
        }

        public Builder nextProcessDelay(Duration nextProcessDelay) {
            this.nexProcessDelay = nextProcessDelay;
            return this;
        }

        public Builder pollingInterval(Duration pollingInterval) {
            this.pollingInterval = pollingInterval;
            return this;
        }

        public Builder pullBatchSize(Long pullBatchSize) {
            this.pullBatchSize = pullBatchSize;
            return this;
        }

        public QueueConfig build() {
            return new QueueConfig(this);
        }
    }
}
