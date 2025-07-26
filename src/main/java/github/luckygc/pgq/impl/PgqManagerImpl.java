package github.luckygc.pgq.impl;

import github.luckygc.pgq.PgqConstants;
import github.luckygc.pgq.QueueDao;
import github.luckygc.pgq.api.BatchMessageHandler;
import github.luckygc.pgq.api.DeadMessageManger;
import github.luckygc.pgq.api.MessageManager;
import github.luckygc.pgq.api.PgqManager;
import github.luckygc.pgq.api.QueueListener;
import github.luckygc.pgq.api.QueueManager;
import github.luckygc.pgq.api.SingleMessageHandler;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class PgqManagerImpl implements PgqManager {

    private static final Logger logger = LoggerFactory.getLogger(PgqManagerImpl.class);

    private static final int NOTIFY_TIMEOUT_MILLIS = Math.toIntExact(TimeUnit.SECONDS.toMillis(25));
    private static final long RECONNECT_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long FIRST_RECONNECT_DELAY_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int VALID_CONNECTION_TIMEOUT_SECONDS = 1;
    private static final long MESSAGE_AVAILABLE_TIMEOUT_MILLIS = Duration.ofMinutes(1).toMillis();

    private final Map<String, QueueManager> queueManagerMap = new ConcurrentHashMap<>();

    // 监听
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final AtomicBoolean listeningFlag = new AtomicBoolean(false);
    private volatile @Nullable PgConnection con;

    private final QueueDao queueDao;
    private final MessageManager messageManager;

    public PgqManagerImpl(String jdbcUrl, String username, String password, JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.queueDao = new QueueDao(jdbcTemplate, transactionTemplate);
        this.messageManager = new MessageManagerImpl(queueDao);
    }

    @Override
    public QueueManager register(String topic) {
        Objects.requireNonNull(topic);

        return queueManagerMap.compute(topic, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("重复注册,topic:%s".formatted(topic));
            }

            PgQueueImpl pgQueue = new PgQueueImpl(queueDao, k);
            return new QueueManagerImpl(pgQueue, null);
        });
    }

    @Override
    public QueueManager register(String topic, QueueListener messageListener) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(messageListener);

        return queueManagerMap.compute(topic, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("重复注册,topic:%s".formatted(topic));
            }

            PgQueueImpl pgQueue = new PgQueueImpl(queueDao, k);
            return new QueueManagerImpl(pgQueue, messageListener);
        });
    }

    @Override
    public QueueManager register(String topic, SingleMessageHandler messageHandler) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(messageHandler);

        return queueManagerMap.compute(topic, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("重复注册,topic:%s".formatted(topic));
            }

            PgQueueImpl pgQueue = new PgQueueImpl(queueDao, k);
            QueueListener messageListener = new SingleMessageProcessor(messageManager, messageHandler);
            return new QueueManagerImpl(pgQueue, messageListener);
        });
    }

    @Override
    public QueueManager register(String topic, BatchMessageHandler messageHandler) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(messageHandler);

        return queueManagerMap.compute(topic, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("重复注册,topic:%s".formatted(topic));
            }

            PgQueueImpl pgQueue = new PgQueueImpl(queueDao, k);
            QueueListener messageListener = new BatchMessageQueueListener(messageHandler);
            return new QueueManagerImpl(pgQueue, messageListener);
        });
    }

    @Override
    public @Nullable QueueManager getQueueManager(String topic) {
        return queueManagerMap.get(topic);
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
        if (!listeningFlag.compareAndSet(false, true)) {
            throw new IllegalStateException("队列正在监听");
        }

        connect();
        listenLoop();
    }

    @Override
    public void stopListen() {
        if (!listeningFlag.compareAndSet(true, false)) {
            throw new IllegalStateException("监听器未在运行");
        }
    }

    private void listenLoop() {
        Thread listenerThread = new Thread(() -> {
            while (listeningFlag.get()) {
                try {
                    checkConnection();
                    PGNotification[] notifications = Objects.requireNonNull(con)
                            .getNotifications(NOTIFY_TIMEOUT_MILLIS);
                    if (notifications == null) {
                        continue;
                    }

                    for (PGNotification notification : notifications) {
                        String channel = notification.getName();

                        String payload = notification.getParameter();
                        int pid = notification.getPID();
                        logger.debug("收到消息, channel:{}, payload:{}, pid:{}", channel, payload, pid);

                        handleChannelPayload(payload);
                    }
                } catch (SQLException e) {
                    logger.error("读取通知失败", e);
                    LockSupport.parkNanos(FIRST_RECONNECT_DELAY_NANOS);
                    reconnect();
                }
            }
            closeConnectionQuietly();
        }, "pgq-manager");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    private void handleChannelPayload(String payload) {
        QueueManager queueManager = getQueueManager(payload);
        if (queueManager == null) {
            return;
        }

        QueueListener messageListener = queueManager.messageListener();
        if (messageListener == null) {
            return;
        }

        long start = System.currentTimeMillis();
        messageListener.onMessageAvailable(queueManager.queue());
        long end = System.currentTimeMillis();
        if ((end - start) > MESSAGE_AVAILABLE_TIMEOUT_MILLIS) {
            logger.warn("onMessageAvailable方法执行时间过长,请不要阻塞调用, topic:{}", payload);
        }
    }

    private void connect() throws SQLException {
        Connection raw = DriverManager.getConnection(jdbcUrl, username, password);
        con = raw.unwrap(PgConnection.class);

        try (Statement statement = Objects.requireNonNull(con).createStatement()) {
            statement.execute("LISTEN %s".formatted(PgqConstants.CHANNEL_NAME));
        }

        logger.debug("已建立连接,正在监听通道: {}", PgqConstants.CHANNEL_NAME);
    }

    private void reconnect() {
        closeConnectionQuietly();
        int attempt = 1;
        while (listeningFlag.get()) {
            try {
                logger.debug("尝试重新监听, 次数:{}", attempt);
                connect();
                break;
            } catch (SQLException e) {
                logger.error("尝试重新监听失败", e);
                LockSupport.parkNanos(RECONNECT_RETRY_DELAY_NANOS);
                attempt++;
            }
        }
    }

    private void checkConnection() throws SQLException {
        if (con == null || !Objects.requireNonNull(con).isValid(VALID_CONNECTION_TIMEOUT_SECONDS)) {
            reconnect();
        }
    }

    /**
     * 静默关闭连接
     */
    private void closeConnectionQuietly() {
        if (con != null) {
            try {
                Objects.requireNonNull(con).close();
            } catch (SQLException e) {
                logger.info("关闭连接时发生异常", e);
            }
        }
    }
}
