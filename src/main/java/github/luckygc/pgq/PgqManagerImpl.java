package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageListener;
import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.PgqManager;
import github.luckygc.pgq.api.ProcessingMessageManager;
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

    private final Map<String, PgQueue> queueMap = new ConcurrentHashMap<>();

    // 监听
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final AtomicBoolean listeningFlag = new AtomicBoolean(false);

    @Nullable
    private volatile PgConnection con = null;

    private final QueueDao queueDao;
    private final ProcessingMessageManager processingMessageManager;

    public PgqManagerImpl(String jdbcUrl, String username, String password, JdbcTemplate jdbcTemplate,
            TransactionTemplate transactionTemplate) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.queueDao = new QueueDao(jdbcTemplate, transactionTemplate);
        this.processingMessageManager = new ProcessingMessageManagerImpl(queueDao);
    }

    @Override
    public PgQueue registerQueue(String topic) {
        Objects.requireNonNull(topic);

        return queueMap.compute(topic, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("重复注册,topic:%s".formatted(topic));
            }

            return new PgQueueImpl(queueDao, k, processingMessageManager, null);
        });
    }

    @Override
    public PgQueue registerQueue(String topic, MessageListener messageListener) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(messageListener);

        return queueMap.compute(topic, (k, v) -> {
            if (v != null) {
                throw new IllegalStateException("重复注册,topic:%s".formatted(topic));
            }

            return new PgQueueImpl(queueDao, k, processingMessageManager, messageListener);
        });
    }

    @Override
    public @Nullable PgQueue getQueue(String topic) {
        return queueMap.get(topic);
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
                    PGNotification[] notifications = con.getNotifications(NOTIFY_TIMEOUT_MILLIS);
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
        PgQueue queue = getQueue(payload);
        if (queue == null) {
            return;
        }

        MessageListener messageListener = queue.messageListener();
        if (messageListener != null) {
            long start = System.currentTimeMillis();
            messageListener.onMessageAvailable();
            long end = System.currentTimeMillis();
            if ((end - start) > MESSAGE_AVAILABLE_TIMEOUT_MILLIS) {
                logger.warn("onMessageAvailable方法执行时间过长,请不要阻塞调用, topic:{}", payload);
            }
        }
    }

    private void connect() throws SQLException {
        Connection raw = DriverManager.getConnection(jdbcUrl, username, password);
        con = raw.unwrap(PgConnection.class);

        try (Statement statement = con.createStatement()) {
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
        if (con == null || !con.isValid(VALID_CONNECTION_TIMEOUT_SECONDS)) {
            reconnect();
        }
    }

    /**
     * 静默关闭连接
     */
    private void closeConnectionQuietly() {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                logger.info("关闭连接时发生异常", e);
            }
        }
    }
}
