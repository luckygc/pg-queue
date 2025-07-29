package github.luckygc.pgq;

import github.luckygc.pgq.api.callback.MessageAvailableCallback;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgListener {

    private static final Logger log = LoggerFactory.getLogger(PgListener.class);

    private static final int LISTEN_CHANNEL_TIMEOUT_MILLIS = Math.toIntExact(TimeUnit.SECONDS.toMillis(20));
    private static final long RECONNECT_RETRY_DELAY_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long FIRST_RECONNECT_DELAY_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int VALID_CONNECTION_TIMEOUT_SECONDS = 1;

    // 监听
    private final String channel;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final MessageAvailableCallback callback;

    private final AtomicBoolean runningFlag = new AtomicBoolean(false);
    private volatile @Nullable PgConnection con;

    public PgListener(String channel, String jdbcUrl, String username, String password,
            MessageProcessorDispatcher callback) {
        this.channel = Objects.requireNonNull(channel);
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl);
        this.username = Objects.requireNonNull(username);
        this.password = password;
        this.callback = Objects.requireNonNull(callback);
    }

    public void startListen() throws SQLException {
        if (!runningFlag.compareAndSet(false, true)) {
            throw new IllegalStateException("队列正在监听");
        }

        connectAndStartListenChannel();

        Thread loopThread = new Thread(this::listenChannel, "pgq-channel-listener");
        loopThread.setDaemon(true);
        loopThread.start();
    }

    public void stopListen() {
        if (!runningFlag.compareAndSet(true, false)) {
            throw new IllegalStateException("监听器未在运行");
        }
    }

    private void listenChannel() {
        while (runningFlag.get()) {
            try {
                checkConnection();
                PGNotification[] notifications = Objects.requireNonNull(con)
                        .getNotifications(LISTEN_CHANNEL_TIMEOUT_MILLIS);
                if (notifications == null) {
                    continue;
                }

                for (PGNotification notification : notifications) {
                    String topic = notification.getParameter();
                    int pid = notification.getPID();
                    log.debug("收到消息, topic:{}, pid:{}", topic, pid);

                    try {
                        callback.onMessageAvailable(topic);
                    } catch (Throwable t) {
                        log.error("消息可用回调失败", t);
                    }
                }
            } catch (SQLException e) {
                log.error("读取通知失败", e);
                LockSupport.parkNanos(FIRST_RECONNECT_DELAY_NANOS);
                reconnect();
            }
        }
        closeConnectionQuietly();
    }

    private void connectAndStartListenChannel() throws SQLException {
        Connection raw = DriverManager.getConnection(jdbcUrl, username, password);
        con = raw.unwrap(PgConnection.class);

        try (Statement statement = Objects.requireNonNull(con).createStatement()) {
            statement.execute("LISTEN %s".formatted(channel));
        }

        log.debug("已建立连接,正在监听通道: {}", channel);
    }

    private void reconnect() {
        closeConnectionQuietly();
        int attempt = 1;
        while (runningFlag.get()) {
            try {
                log.debug("尝试重新监听, 次数:{}", attempt);
                connectAndStartListenChannel();
                break;
            } catch (SQLException e) {
                log.error("尝试重新监听失败", e);
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
                log.info("关闭连接时发生异常", e);
            }
        }
    }
}
