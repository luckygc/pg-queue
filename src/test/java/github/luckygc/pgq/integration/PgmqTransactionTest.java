package github.luckygc.pgq.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.impl.PgmqManagerImpl;
import github.luckygc.pgq.model.Message;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@DisplayName("PGMQ事务测试")
class PgmqTransactionTest extends BaseIntegrationTest {

    private PgmqManager pgmqManager;
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void setUp() {
        pgmqManager = new PgmqManagerImpl(jdbcTemplate);
        var dataSourceTransactionManager = new DataSourceTransactionManager(dataSource);
        transactionTemplate = new TransactionTemplate(dataSourceTransactionManager);
    }

    @AfterEach
    void tearDown() {
        if (pgmqManager != null) {
            pgmqManager.shutdown();
        }
    }

    @Test
    @DisplayName("应该支持事务中的消息发送")
    void shouldSupportTransactionalMessageSending() {
        transactionTemplate.executeWithoutResult(status -> {
            String topic = "transactional-send-topic";

            // 在事务中发送消息
            pgmqManager.queue().send(topic, "transactional message 1");
            pgmqManager.queue().send(topic, "transactional message 2");

            // 在事务提交前，消息应该已经可见（因为使用的是同一个连接）
            assertThat(countPendingMessages(topic)).isEqualTo(2);

            // 事务会在方法结束时自动提交
        });
    }

    @Test
    @DisplayName("应该支持事务回滚时消息不发送")
    void shouldSupportTransactionalRollback() {
        String topic = "rollback-topic";
        
        try {
            transactionTemplate.execute(status -> {
                pgmqManager.queue().send(topic, "message in transaction");
                
                // 在事务中验证消息存在
                assertThat(countPendingMessages(topic)).isEqualTo(1);
                
                // 强制回滚
                status.setRollbackOnly();
                return null;
            });
        } catch (Exception e) {
            // 忽略异常
        }
        
        // 事务回滚后，消息应该不存在
        assertThat(countPendingMessages(topic)).isEqualTo(0);
    }

    @Test
    @DisplayName("应该处理事务中的异常情况")
    void shouldHandleExceptionInTransaction() {
        String topic = "exception-transactional-topic";
        
        assertThatThrownBy(() -> {
            transactionTemplate.execute(status -> {
                pgmqManager.queue().send(topic, "message before exception");
                
                // 验证消息在事务中存在
                assertThat(countPendingMessages(topic)).isEqualTo(1);
                
                // 抛出异常导致事务回滚
                throw new RuntimeException("Transaction exception");
            });
        }).isInstanceOf(RuntimeException.class);
        
        // 事务回滚后消息应该不存在
        assertThat(countPendingMessages(topic)).isEqualTo(0);
    }
}
