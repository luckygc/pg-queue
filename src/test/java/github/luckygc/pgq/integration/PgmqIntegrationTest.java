package github.luckygc.pgq.integration;

import static org.assertj.core.api.Assertions.assertThat;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.impl.PgmqManagerImpl;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("PGMQ集成测试")
class PgmqIntegrationTest extends BaseIntegrationTest {

    private PgmqManager pgmqManager;

    @BeforeEach
    void setUp() {
        pgmqManager = new PgmqManagerImpl(jdbcTemplate);
    }

    @AfterEach
    void tearDown() {
        if (pgmqManager != null) {
            pgmqManager.shutdown();
        }
    }

    @Test
    @DisplayName("应该能够发送和接收单条消息")
    void shouldSendAndReceiveMessage() {
        String topic = "test-topic";
        String payload = "test message";

        // 发送消息
        pgmqManager.queue().send(topic, payload);

        // 接收消息
        Message message = pgmqManager.queue().poll(topic);

        assertThat(message).isNotNull();
        assertThat(message.getTopic()).isEqualTo(topic);
        assertThat(message.getPayload()).isEqualTo(payload);
        assertThat(message.getPriority()).isEqualTo(0);
        assertThat(message.getAttempt()).isEqualTo(1);
    }

    @Test
    @DisplayName("应该能够发送和接收多条消息")
    void shouldSendAndReceiveMultipleMessages() {
        String topic = "multi-topic";
        List<String> payloads = Arrays.asList("message1", "message2", "message3");

        // 发送多条消息
        pgmqManager.queue().send(topic, payloads);

        // 接收消息
        List<Message> messages = pgmqManager.queue().poll(topic, 5);

        assertThat(messages).hasSize(3);
        assertThat(messages.get(0).getPayload()).isEqualTo("message1");
        assertThat(messages.get(1).getPayload()).isEqualTo("message2");
        assertThat(messages.get(2).getPayload()).isEqualTo("message3");
    }

    @Test
    @DisplayName("应该能够处理优先级消息")
    void shouldHandlePriorityMessages() {
        String topic = "priority-topic";

        // 发送不同优先级的消息
        pgmqManager.priorityQueue().send(topic, "low priority", 1);
        pgmqManager.priorityQueue().send(topic, "high priority", 10);
        pgmqManager.priorityQueue().send(topic, "medium priority", 5);

        // 接收消息，应该按优先级排序
        List<Message> messages = pgmqManager.queue().poll(topic, 5);

        assertThat(messages).hasSize(3);
        assertThat(messages.get(0).getPayload()).isEqualTo("high priority");
        assertThat(messages.get(0).getPriority()).isEqualTo(10);
        assertThat(messages.get(1).getPayload()).isEqualTo("medium priority");
        assertThat(messages.get(1).getPriority()).isEqualTo(5);
        assertThat(messages.get(2).getPayload()).isEqualTo("low priority");
        assertThat(messages.get(2).getPriority()).isEqualTo(1);
    }

    @Test
    @DisplayName("应该能够处理延迟消息")
    void shouldHandleDelayedMessages() throws InterruptedException {
        String topic = "delay-topic";
        String payload = "delayed message";

        // 发送延迟消息（2秒后可见）
        pgmqManager.delayQueue().send(topic, payload, Duration.ofSeconds(2));

        // 立即尝试接收，应该没有消息
        Message immediateMessage = pgmqManager.queue().poll(topic);
        assertThat(immediateMessage).isNull();

        // 等待3秒后再次尝试接收
        Thread.sleep(3000);

        // 手动触发定时任务（移动不可见消息到待处理队列）
        triggerScheduledTask();

        Message delayedMessage = pgmqManager.queue().poll(topic);
        assertThat(delayedMessage).isNotNull();
        assertThat(delayedMessage.getPayload()).isEqualTo(payload);
    }

    @Test
    @DisplayName("应该能够使用消息处理器处理消息")
    void shouldProcessMessagesWithHandler() throws InterruptedException {
        String topic = "handler-topic";
        List<String> receivedMessages = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(3);

        MessageHandler handler = new MessageHandler() {
            @Override
            public String topic() {
                return topic;
            }

            @Override
            public void handle(Message message) {
                receivedMessages.add(message.getPayload());
                message.delete();
                latch.countDown();
            }
        };

        // 注册处理器
        pgmqManager.registerHandler(handler);

        // 发送消息
        pgmqManager.queue().send(topic, "message1");
        pgmqManager.queue().send(topic, "message2");
        pgmqManager.queue().send(topic, "message3");

        // 等待消息处理完成
        boolean completed = latch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(receivedMessages).containsExactlyInAnyOrder("message1", "message2", "message3");
    }

    @Test
    @DisplayName("应该能够处理消息重试")
    void shouldHandleMessageRetry() {
        String topic = "retry-topic";
        String payload = "retry message";

        // 发送消息
        pgmqManager.queue().send(topic, payload);

        // 接收消息
        Message message = pgmqManager.queue().poll(topic);
        assertThat(message).isNotNull();

        // 重试消息
        message.retry();

        // 再次接收消息
        Message retryMessage = pgmqManager.queue().poll(topic);
        assertThat(retryMessage).isNotNull();
        assertThat(retryMessage.getPayload()).isEqualTo(payload);
        assertThat(retryMessage.getAttempt()).isEqualTo(2); // 重试会增加attempt
    }

    @Test
    @DisplayName("应该能够处理死信队列")
    void shouldHandleDeadLetterQueue() {
        String topic = "dead-topic";
        String payload = "dead message";

        // 发送消息
        pgmqManager.queue().send(topic, payload);

        // 接收消息
        Message message = pgmqManager.queue().poll(topic);
        assertThat(message).isNotNull();

        // 移动到死信队列
        message.dead();

        // 验证消息已从处理队列中移除
        Message nextMessage = pgmqManager.queue().poll(topic);
        assertThat(nextMessage).isNull();

        // 验证死信队列中有消息
        assertThat(countDeadMessages(topic)).isEqualTo(1);
    }

    @Test
    @DisplayName("应该能够处理并发消息处理")
    void shouldHandleConcurrentProcessing() throws InterruptedException {
        String topic = "concurrent-topic";
        int messageCount = 100;
        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        MessageHandler handler = new MessageHandler() {
            @Override
            public String topic() {
                return topic;
            }

            @Override
            public int threadCount() {
                return 5; // 使用5个线程并发处理
            }

            @Override
            public void handle(Message message) {
                processedCount.incrementAndGet();
                message.delete();
                latch.countDown();
            }
        };

        // 注册处理器
        pgmqManager.registerHandler(handler);

        // 发送大量消息
        for (int i = 0; i < messageCount; i++) {
            pgmqManager.queue().send(topic, "message" + i);
        }

        // 等待所有消息处理完成
        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(processedCount.get()).isEqualTo(messageCount);
    }

    @Test
    @DisplayName("应该能够处理空消息和特殊字符")
    void shouldHandleEmptyAndSpecialCharacterMessages() {
        String topic = "special-topic";

        // 测试空字符串
        pgmqManager.queue().send(topic, "");
        Message emptyMessage = pgmqManager.queue().poll(topic);
        assertThat(emptyMessage).isNotNull();
        assertThat(emptyMessage.getPayload()).isEqualTo("");
        emptyMessage.delete();

        // 测试特殊字符
        String specialPayload = "特殊字符测试: !@#$%^&*()_+{}|:<>?[]\\;'\",./ 中文 🚀 \n\t\r";
        pgmqManager.queue().send(topic, specialPayload);
        Message specialMessage = pgmqManager.queue().poll(topic);
        assertThat(specialMessage).isNotNull();
        assertThat(specialMessage.getPayload()).isEqualTo(specialPayload);
        specialMessage.delete();

        // 测试长消息
        String longPayload = "x".repeat(10000);
        pgmqManager.queue().send(topic, longPayload);
        Message longMessage = pgmqManager.queue().poll(topic);
        assertThat(longMessage).isNotNull();
        assertThat(longMessage.getPayload()).isEqualTo(longPayload);
        longMessage.delete();
    }

    @Test
    @DisplayName("应该能够处理消息处理器异常")
    void shouldHandleMessageHandlerExceptions() throws InterruptedException {
        String topic = "exception-topic";
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        MessageHandler handler = new MessageHandler() {
            @Override
            public String topic() {
                return topic;
            }

            @Override
            public void handle(Message message) {
                try {
                    processedCount.incrementAndGet();
                    if (message.getPayload().contains("error")) {
                        exceptionCount.incrementAndGet();
                        throw new RuntimeException("模拟处理异常");
                    }
                    message.delete();
                } catch (Exception e) {
                    // 异常处理逻辑
                    if (message.getAttempt() >= 3) {
                        message.dead();
                    } else {
                        message.retry();
                    }
                } finally {
                    latch.countDown();
                }
            }
        };

        pgmqManager.registerHandler(handler);

        // 发送正常消息和异常消息
        pgmqManager.queue().send(topic, "normal message");
        pgmqManager.queue().send(topic, "error message");
        pgmqManager.queue().send(topic, "another normal");

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        // 异常总共尝试3次，其他的各一次
        assertThat(processedCount.get()).isEqualTo(5);
        assertThat(exceptionCount.get()).isEqualTo(3);
    }
}
