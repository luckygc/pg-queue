package github.luckygc.pgq.impl;

import github.luckygc.pgq.PgListener;
import github.luckygc.pgq.PgqConstants;
import github.luckygc.pgq.QueueDao;
import github.luckygc.pgq.api.BatchMessageHandler;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.DeadMessageManger;
import github.luckygc.pgq.api.MessageManager;
import github.luckygc.pgq.api.QueueListener;
import github.luckygc.pgq.api.QueueManager;
import github.luckygc.pgq.api.SingleMessageHandler;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueManagerImpl implements QueueManager {

    private static final Logger log = LoggerFactory.getLogger(QueueManagerImpl.class);

    private static final int LISTEN_CHANNEL_TIMEOUT_MILLIS = Math.toIntExact(TimeUnit.SECONDS.toMillis(20));
    private static final long RECONNECT_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long FIRST_RECONNECT_DELAY_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int VALID_CONNECTION_TIMEOUT_SECONDS = 1;
    private static final long MESSAGE_AVAILABLE_TIMEOUT_MILLIS = Duration.ofMinutes(1).toMillis();

    private final Map<String, DatabaseQueue> queueMap = new ConcurrentHashMap<>();
    private final Map<String, QueueListener> listenerMap = new ConcurrentHashMap<>();

    private final PgListener pgListener;
    private final QueueDao queueDao;
    private final MessageManager messageManager;

    public QueueManagerImpl(String jdbcUrl, String username, String password, JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate) {
        this.queueDao = new QueueDao(jdbcTemplate, transactionTemplate);
        this.messageManager = new MessageManagerImpl(queueDao);
        pgListener = new PgListener(PgqConstants.TOPIC_CHANNEL, jdbcUrl, username, password, this::dispatch);
    }

    @Override
    public DatabaseQueue queue(String topic) {
        return queueMap.compute(topic, (k, v) -> {
            if (v != null) {
                return v;
            }

            return new DatabaseQueueImpl(queueDao, k);
        });
    }

    @Override
    public void registerListener(QueueListener messageListener) {
        QueueListener queueListener = listenerMap.putIfAbsent(messageListener.topic(), messageListener);
        if (queueListener != null) {
            throw new IllegalStateException("当前已存在topic[{}]的监听器");
        }
    }

    @Override
    public void registerMessageHandler(SingleMessageHandler messageHandler) {
        DatabaseQueue queue = queue(messageHandler.topic());
        SingleMessageProcessor processor = new SingleMessageProcessor(queue, messageManager, messageHandler);
        registerListener(processor);
    }

    @Override
    public void registerMessageHandler(BatchMessageHandler messageHandler) {
        DatabaseQueue queue = queue(messageHandler.topic());
        BatchMessageProcessor processor = new BatchMessageProcessor(queue, messageManager, messageHandler);
        registerListener(processor);
    }

    @Override
    public @Nullable QueueListener listener(String topic) {
        return listenerMap.get(topic);
    }

    @Override
    public MessageManager messageManager() {
        return messageManager;
    }

    @Override
    public DeadMessageManger deadMessageManager() {
        return null;
    }

    @Override
    public void startListen() throws SQLException {
        pgListener.start();
    }

    @Override
    public void stopListen() {
        pgListener.stop();
    }

    private void dispatch(String topic) {
        QueueListener listener = listenerMap.get(topic);
        if (listener == null) {
            return;
        }

        long start = System.currentTimeMillis();
        listener.onMessageAvailable();
        long end = System.currentTimeMillis();
        if ((end - start) > MESSAGE_AVAILABLE_TIMEOUT_MILLIS) {
            log.warn("onMessageAvailable方法执行时间过长,请不要阻塞调用, topic:{}", topic);
        }
    }
}
