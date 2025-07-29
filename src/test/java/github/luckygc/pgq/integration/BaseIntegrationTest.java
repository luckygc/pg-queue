package github.luckygc.pgq.integration;

import java.io.IOException;
import java.nio.file.Files;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.ResourceUtils;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 基础集成测试类，提供共享的PostgreSQL容器和数据库初始化 所有集成测试都应该继承这个类以提高测试效率
 */
@Testcontainers
public abstract class BaseIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("test")
            .withUsername("test")
            .withPassword("test")
            .withReuse(true); // 启用容器重用以提高性能

    protected static JdbcTemplate jdbcTemplate;
    protected static DriverManagerDataSource dataSource;

    @BeforeAll
    static void setUpDatabase() throws IOException {
        // 创建数据源 - 只创建一次
        dataSource = new DriverManagerDataSource();
        dataSource.setUrl(postgres.getJdbcUrl());
        dataSource.setUsername(postgres.getUsername());
        dataSource.setPassword(postgres.getPassword());
        dataSource.setDriverClassName("org.postgresql.Driver");

        jdbcTemplate = new JdbcTemplate(dataSource);

        // 执行DDL脚本 - 只执行一次
        String ddlScript = Files.readString(ResourceUtils.getFile("classpath:ddl.sql").toPath());
        jdbcTemplate.execute(ddlScript);
    }

    @AfterAll
    static void afterAll() {
        postgres.close();
    }

    @AfterEach
    void cleanUpData() {
        // 每个测试后清理数据，但保留表结构
        String sql = """
                TRUNCATE TABLE pgmq_pending_queue, pgmq_processing_queue,
                pgmq_invisible_queue, pgmq_dead_queue RESTART IDENTITY
                """;
        jdbcTemplate.execute(sql);
    }

    /**
     * 获取数据库连接URL
     */
    protected static String getJdbcUrl() {
        return postgres.getJdbcUrl();
    }

    /**
     * 获取数据库用户名
     */
    protected static String getUsername() {
        return postgres.getUsername();
    }

    /**
     * 获取数据库密码
     */
    protected static String getPassword() {
        return postgres.getPassword();
    }

    /**
     * 验证数据库中指定表的记录数
     */
    protected int countRecords(String tableName, String whereClause, Object... params) {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sql += " WHERE " + whereClause;
        }
        return jdbcTemplate.queryForObject(sql, Integer.class, params);
    }

    /**
     * 验证待处理队列中的消息数量
     */
    protected int countPendingMessages(String topic) {
        return countRecords("pgmq_pending_queue", "topic = ?", topic);
    }

    /**
     * 验证处理中队列的消息数量
     */
    protected int countProcessingMessages(String topic) {
        return countRecords("pgmq_processing_queue", "topic = ?", topic);
    }

    /**
     * 验证不可见队列的消息数量
     */
    protected int countInvisibleMessages(String topic) {
        return countRecords("pgmq_invisible_queue", "topic = ?", topic);
    }

    /**
     * 验证死信队列的消息数量
     */
    protected int countDeadMessages(String topic) {
        return countRecords("pgmq_dead_queue", "topic = ?", topic);
    }

    /**
     * 手动触发定时任务（移动超时和可见消息到待处理队列）
     */
    protected void triggerScheduledTask() {
        jdbcTemplate.query("SELECT pgmq_move_timeout_and_visible_msg_to_pending_then_notify()", rs -> null);
    }
}
