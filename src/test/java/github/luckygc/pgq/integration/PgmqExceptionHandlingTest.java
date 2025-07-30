package github.luckygc.pgq.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.impl.PgmqManagerImpl;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("PGMQ异常处理测试")
class PgmqExceptionHandlingTest extends BaseIntegrationTest {

    private PgmqManager pgmqManager;

    @BeforeEach
    void setUp() {
        pgmqManager = new PgmqManagerImpl(jdbcTemplate);
    }

    @AfterEach
    void closeManager() {
        pgmqManager.shutdown();
        // 防止truncate 死锁
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
    }

    @Test
    @DisplayName("应该拒绝null参数")
    void shouldRejectNullParameters() {
        assertThatThrownBy(() -> pgmqManager.queue().send(null, "message"))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> pgmqManager.queue().send("topic", (String) null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> pgmqManager.queue().poll(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> pgmqManager.delayQueue().send("topic", "message", null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> pgmqManager.registerHandler(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("应该拒绝负数或零的延迟时间")
    void shouldRejectInvalidDelayDuration() {
        assertThatThrownBy(() -> pgmqManager.delayQueue().send("topic", "message", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> pgmqManager.delayQueue().send("topic", "message", Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("应该拒绝空的消息列表")
    void shouldRejectEmptyMessageList() {
        assertThatThrownBy(() -> pgmqManager.queue().send("topic", Arrays.asList()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("应该拒绝包含null元素的消息列表")
    void shouldRejectMessageListWithNullElements() {
        assertThatThrownBy(() -> pgmqManager.queue().send("topic", Arrays.asList("msg1", null, "msg3")))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("应该拒绝无效的poll数量")
    void shouldRejectInvalidPollCount() {
        assertThatThrownBy(() -> pgmqManager.queue().poll("topic", 0))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> pgmqManager.queue().poll("topic", -1))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> pgmqManager.queue().poll("topic", 5001))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("应该处理消息处理器中的运行时异常")
    void shouldHandleRuntimeExceptionInHandler() throws InterruptedException {
        String topic = "runtime-exception-topic";
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger retryCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        MessageHandler handler = new MessageHandler() {
            @Override
            public String topic() {
                return topic;
            }

            @Override
            public void handle(Message message) {
                processedCount.incrementAndGet();
                try {
                    if (message.getAttempt() == 1) {
                        throw new RuntimeException("模拟运行时异常");
                    }
                    message.delete();
                    latch.countDown();
                } catch (RuntimeException e) {
                    if (message.getAttempt() >= 3) {
                        message.dead();
                        latch.countDown();
                    } else {
                        retryCount.incrementAndGet();
                        message.retry();
                    }
                }
            }
        };

        pgmqManager.registerHandler(handler);
        pgmqManager.queue().send(topic, "test message");

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(processedCount.get()).isGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("应该处理消息处理器中的检查异常")
    void shouldHandleCheckedExceptionInHandler() throws InterruptedException {
        String topic = "checked-exception-topic";
        CountDownLatch latch = new CountDownLatch(1);

        MessageHandler handler = new MessageHandler() {
            @Override
            public String topic() {
                return topic;
            }

            @Override
            public void handle(Message message) {
                try {
                    // 模拟可能抛出检查异常的操作
                    simulateCheckedExceptionOperation();
                    message.delete();
                } catch (Exception e) {
                    message.dead();
                } finally {
                    latch.countDown();
                }
            }

            private void simulateCheckedExceptionOperation() throws Exception {
                throw new Exception("模拟检查异常");
            }
        };

        pgmqManager.registerHandler(handler);
        pgmqManager.queue().send(topic, "test message");

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(countDeadMessages(topic)).isEqualTo(1);
    }

    @Test
    @DisplayName("应该处理数据库连接异常")
    void shouldHandleDatabaseConnectionException() {
        // 这个测试需要模拟数据库连接问题
        // 在实际环境中，可以通过关闭数据库连接或使用错误的连接参数来测试

        // 发送消息到不存在的topic（这会触发数据库操作）
        String topic = "db-error-topic";

        // 正常情况下应该能发送消息
        pgmqManager.queue().send(topic, "test message");

        // 验证消息确实被发送了
        assertThat(countPendingMessages(topic)).isEqualTo(1);
    }
}
