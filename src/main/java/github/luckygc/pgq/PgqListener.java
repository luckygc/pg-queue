package github.luckygc.pgq;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;
import org.postgresql.PGNotification;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PgqListener {

    private static final Logger logger = LoggerFactory.getLogger(PgqListener.class);

    // 连接配置
    private final String jdbcUrl;
    private final String username;
    private final String password;

    private final AtomicBoolean isListening = new AtomicBoolean(false);
    private Thread listenerThread;
    private volatile Connection con;

    public PgqListener(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * 开始监听队列
     */
    public void listen() throws SQLException {
        if (!isListening.compareAndSet(false, true)) {
            throw new IllegalStateException("队列正在监听");
        }

        Runnable runnable = () -> {
            try {
                ensureConnection();

                try (Statement statement = con.createStatement()) {
                    statement.execute("LISTEN __pgq_queue__");
                }

                while (isConnectionValid()) {
                    ensureConnection();
                    PgConnection pgCon = (PgConnection) con;
                    PGNotification[] notifications = pgCon.getNotifications(25);
                    if (notifications != null) {

                    }

                }

                logger.info("开始监听队列通知");
            } catch (SQLException e) {
                logger.error("监听失败", e);
                throw new RuntimeException(e);
            }
        };

        listenerThread = new Thread(runnable, "pgq-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    /**
     * 停止监听队列
     */
    public void unlisten() throws SQLException {
        if (!isListening.compareAndSet(true, false)) {
            throw new IllegalStateException("监听器未在运行");
        }

        listenerThread.interrupt();
        closeConnectionSilently();
    }

    private void reconnect() {

    }

    /**
     * 确保连接有效，如果无效则重新建立连接
     */
    private void ensureConnection() throws SQLException {
        if (isConnectionValid()) {
            return;
        }

        establishConnection();
    }

    /**
     * 建立数据库连接（带重试机制）
     */
    private void establishConnection() throws SQLException {
        SQLException lastException = null;
        int maxRetryCount = 10;
        long baseDelayMs = 10000; // 首次10秒

        for (int attempt = 1; attempt <= maxRetryCount; attempt++) {
            try {
                logger.info("尝试建立数据库连接，第 {} 次尝试", attempt);
                closeConnectionSilently();
                con = DriverManager.getConnection(jdbcUrl, username, password);
                return;
            } catch (SQLException e) {
                lastException = e;
                logger.warn("第 {} 次连接尝试失败: {}", attempt, e.getMessage());

                if (attempt < maxRetryCount) {
                    try {
                        long delayMs = baseDelayMs * (1L << (attempt - 1)); // 指数退避
                        logger.info("等待 {} 毫秒后进行下次重试", delayMs);
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("连接重试被中断", ie);
                    }
                }
            }
        }

        throw new SQLException("经过 " + maxRetryCount + " 次尝试后仍无法建立数据库连接", lastException);
    }

    /**
     * 检查连接健康状态
     */
    private boolean isConnectionValid() {
        if (con == null) {
            return false;
        }

        try {
            return !con.isClosed() && con.isValid(5);
        } catch (SQLException e) {
            logger.debug("连接健康检查失败", e);
            return false;
        }
    }

    /**
     * 静默关闭连接
     */
    private void closeConnectionSilently() {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                logger.debug("关闭连接时发生异常", e);
            }
        }
    }
}
