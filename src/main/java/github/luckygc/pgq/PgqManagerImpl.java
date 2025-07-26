package github.luckygc.pgq;

import github.luckygc.pgq.api.MessageListener;
import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.PgqManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgqManagerImpl implements PgqManager {

    private static final Logger logger = LoggerFactory.getLogger(PgqManagerImpl.class);

    private static final int notifyTimeoutMills = Math.toIntExact(TimeUnit.SECONDS.toMillis(25));
    private static final long reconnectRetryDelayNanos = TimeUnit.SECONDS.toNanos(10);
    private static final long firstReconnectDelayNanos = TimeUnit.SECONDS.toNanos(5);
    private static final int validConTimeoutSeconds = 1;

    // 连接配置
    private final String jdbcUrl;
    private final String username;
    private final String password;

    private final AtomicBoolean listeningFlag = new AtomicBoolean(false);
    private volatile PgConnection con;

    public PgqManagerImpl(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public PgQueue registerQueue(String topic) {
        return null;
    }

    @Override
    public @Nullable PgQueue getQueue(String topic) {
        return null;
    }

    @Override
    public QueueDao queueDao() {
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

    }

    private void listenLoop() {
        Thread listenerThread = new Thread(() -> {
            while (listeningFlag.get()) {
                try {
                    checkConnection();
                    PGNotification[] notifications = con.getNotifications(notifyTimeoutMills);
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
                    LockSupport.parkNanos(firstReconnectDelayNanos);
                    reconnect();
                }
            }
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
        LocalDateTime start = LocalDateTime.now();
        if (messageListener != null) {
            messageListener.onMessageAvailable();
        }

        LocalDateTime end = LocalDateTime.now();
        if (end.isAfter(start.plus(Duration.ofMinutes(1)))) {
            logger.warn("onMessageAvailable方法执行时间过长,请不要阻塞调用, topic:{}", payload);
        }
    }

    /**
     * 停止监听队列
     */
    public void stop() {
        if (!listeningFlag.compareAndSet(true, false)) {
            throw new IllegalStateException("监听器未在运行");
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
                LockSupport.parkNanos(reconnectRetryDelayNanos);
                attempt++;
            }
        }
    }

    private void checkConnection() throws SQLException {
        if (con == null || !con.isValid(validConTimeoutSeconds)) {
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
