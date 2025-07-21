package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageHandler;
import github.luckygc.pgq.api.MessageSerializable;
import github.luckygc.pgq.config.QueueConfig;
import java.time.Duration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;

public class PgQueueBuilder<M> {

    QueueDao queueDao;
    TransactionTemplate transactionTemplate;
    final QueueConfig.Builder configBuilder = new QueueConfig.Builder();
    MessageSerializable<M> messageSerializer;
    MessageHandler<M> messageHandler;
    Integer handlerCount;

    public static <T> PgQueueBuilder<T> create() {
        return new PgQueueBuilder<>();
    }

    public TxTemplateGather jdbcClient(JdbcClient jdbcClient) {
        this.queueDao = new QueueDao(jdbcClient);
        return new TxTemplateGather();
    }

    public class TxTemplateGather {

        public TopicGather transactionTemplate(TransactionTemplate transactionTemplate) {
            PgQueueBuilder.this.transactionTemplate = transactionTemplate;
            return new TopicGather();
        }
    }

    public class TopicGather {

        public MessageSerializerGather topic(String topic) {
            configBuilder.topic(topic);
            return new MessageSerializerGather();
        }
    }

    public class MessageSerializerGather {

        public MessageHandlerGather messageSerializer(MessageSerializable<M> messageSerializer) {
            PgQueueBuilder.this.messageSerializer = messageSerializer;
            return new MessageHandlerGather();
        }
    }

    public class MessageHandlerGather {

        public FinalGather messageHandler(MessageHandler<M> messageHandler) {
            PgQueueBuilder.this.messageHandler = messageHandler;
            return new FinalGather();
        }
    }

    public class FinalGather {

        /**
         * @param handlerCount 消息处理器数量
         */
        public FinalGather handlerCount(Integer handlerCount) {
            PgQueueBuilder.this.handlerCount = handlerCount;
            return this;
        }

        /**
         * @param pollingInterval 轮询间隔
         */
        public FinalGather pollingInterval(Duration pollingInterval) {
            configBuilder.pollingInterval(pollingInterval);
            return this;
        }

        /**
         * @param pullBatchSize 一次拉取消息数量
         */
        public FinalGather pullBatchSize(Long pullBatchSize) {
            configBuilder.pullBatchSize(pullBatchSize);
            return this;
        }

        /**
         * @param firstProcessDelay 首次处理延迟
         */
        public FinalGather firstProcessDelay(Duration firstProcessDelay) {
            configBuilder.firstProcessDelay(firstProcessDelay);
            return this;
        }

        /**
         * @param nextProcessDelay 下一次重试处理延迟
         */
        public FinalGather nextProcessDelay(Duration nextProcessDelay) {
            configBuilder.nextProcessDelay(nextProcessDelay);
            return this;
        }

        public FinalGather maxAttempt(Integer maxAttempt) {
            configBuilder.maxAttempt(maxAttempt);
            return this;
        }

        public PgQueue<M> build() {
            QueueConfig config = configBuilder.build();
            return new PgQueueImpl<>(queueDao, config, messageSerializer, messageHandler, handlerCount);
        }
    }
}
