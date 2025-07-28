package github.luckygc.pgq;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * 数据库测试基类
 * 提供PostgreSQL测试容器和基础的数据库操作工具
 */
@Testcontainers
public abstract class DatabaseTestBase {

    @Container
    protected static final PostgreSQLContainer<?> POSTGRES_CONTAINER = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("pgq_test")
            .withUsername("test")
            .withPassword("test")
            .withInitScript("ddl.sql");

    protected static DataSource dataSource;
    protected static JdbcTemplate jdbcTemplate;
    protected static TransactionTemplate transactionTemplate;

    @BeforeAll
    static void setUpDatabase() {
        // 创建数据源
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES_CONTAINER.getJdbcUrl());
        config.setUsername(POSTGRES_CONTAINER.getUsername());
        config.setPassword(POSTGRES_CONTAINER.getPassword());
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        dataSource = new HikariDataSource(config);
        jdbcTemplate = new JdbcTemplate(dataSource);
        
        // 创建事务模板
        DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @AfterAll
    static void tearDownDatabase() {
        if (dataSource instanceof HikariDataSource hikariDataSource) {
            hikariDataSource.close();
        }
    }

    @BeforeEach
    void cleanUpTables() {
        // 清理所有队列表的数据
        jdbcTemplate.execute("TRUNCATE TABLE pgq_pending_queue RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE pgq_processing_queue RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE pgq_invisible_queue RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE pgq_complete_queue RESTART IDENTITY CASCADE");
        jdbcTemplate.execute("TRUNCATE TABLE pgq_dead_queue RESTART IDENTITY CASCADE");
        
        // 重置序列
        jdbcTemplate.execute("ALTER SEQUENCE pgq_message_seq RESTART WITH 1");
    }

    /**
     * 获取数据源
     */
    protected DataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取JdbcTemplate
     */
    protected JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    /**
     * 获取事务模板
     */
    protected TransactionTemplate getTransactionTemplate() {
        return transactionTemplate;
    }

    /**
     * 执行SQL脚本
     */
    protected void executeSqlScript(String scriptPath) {
        try {
            ClassPathResource resource = new ClassPathResource(scriptPath);
            String sql = Files.readString(Paths.get(resource.getURI()), StandardCharsets.UTF_8);
            jdbcTemplate.execute(sql);
        } catch (IOException e) {
            throw new RuntimeException("无法读取SQL脚本: " + scriptPath, e);
        }
    }

    /**
     * 获取表中的记录数
     */
    protected int getTableCount(String tableName) {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, Integer.class);
    }

    /**
     * 检查表是否为空
     */
    protected boolean isTableEmpty(String tableName) {
        return getTableCount(tableName) == 0;
    }
} 
