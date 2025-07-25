package github.luckygc.pgq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgqListener {

    private static final Logger logger = LoggerFactory.getLogger(PgqListener.class);

    public static final String CHANNEL = "__pgq_queue__";

    private static final int notifyTimeoutMills = Math.toIntExact(TimeUnit.SECONDS.toMillis(25));
    private static final long reconnectRetryDelay = TimeUnit.SECONDS.toNanos(10);
    private static final long firstReconnectDelay = TimeUnit.SECONDS.toNanos(5);

    // 连接配置
    private final String jdbcUrl;
    private final String username;
    private final String password;

    private final AtomicBoolean runningFlag = new AtomicBoolean(false);
    private volatile PgConnection con;

    public PgqListener(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * 开始监听队列
     */
    public void start() throws SQLException {
        if (!runningFlag.compareAndSet(false, true)) {
            throw new IllegalStateException("队列正在监听");
        }

        connect();
        listenLoop();
    }

    private void listenLoop() {
        Thread listenerThread = new Thread(() -> {
            while (runningFlag.get()) {
                try {
                    checkConnection();
                    PGNotification[] notifications = con.getNotifications(notifyTimeoutMills);
                    if (notifications != null) {
                        for (PGNotification notification : notifications) {
                            String parameter = notification.getParameter();
                        }
                    }
                } catch (SQLException e) {
                    logger.error("读取通知失败", e);
                    LockSupport.parkNanos(firstReconnectDelay);
                    reconnect();
                }

            }
        }, "pgq-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    /**
     * 停止监听队列
     */
    public void stop() {
        if (!runningFlag.compareAndSet(true, false)) {
            throw new IllegalStateException("监听器未在运行");
        }
    }

    private void connect() throws SQLException {
        Connection raw = DriverManager.getConnection(jdbcUrl, username, password);
        con = raw.unwrap(PgConnection.class);

        try (Statement statement = con.createStatement()) {
            statement.execute("LISTEN __pgq_queue__");
        }

        logger.debug("已建立连接,正在监听通道__pgq_queue__");
    }

    private void reconnect() {
        closeConnectionQuietly();
        int attempt = 1;
        while (runningFlag.get()) {
            try {
                logger.debug("尝试重新监听, 次数:{}", attempt);
                connect();
                break;
            } catch (SQLException e) {
                logger.error("尝试重新监听失败", e);
                LockSupport.parkNanos(reconnectRetryDelay);
                attempt++;
            }
        }
    }

    private void checkConnection() throws SQLException {
        if (con == null || !con.isValid(1)) {
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
