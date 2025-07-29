package github.luckygc.pgq.integration;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.impl.PgmqManagerImpl;
import github.luckygc.pgq.model.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PgmqConcurrencyTest extends BaseIntegrationTest {

    private PgmqManager pgmqManager;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        pgmqManager = new PgmqManagerImpl(jdbcTemplate);
        executorService = Executors.newFixedThreadPool(10);
    }

    @AfterEach
    void tearDown() {
        if (pgmqManager != null) {
            pgmqManager.shutdown();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    void shouldHandleConcurrentMessageSending() throws InterruptedException {
        String topic = "concurrent-send-topic";
        int threadCount = 10;
        int messagesPerThread = 100;
        CountDownLatch latch = new CountDownLatch(threadCount);

        // 并发发送消息
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < messagesPerThread; j++) {
                        pgmqManager.queue().send(topic, "message-" + threadId + "-" + j);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // 验证所有消息都已发送
        assertThat(countPendingMessages(topic)).isEqualTo(threadCount * messagesPerThread);
    }

    @Test
    void shouldHandleConcurrentMessagePolling() throws InterruptedException {
        String topic = "concurrent-poll-topic";
        int messageCount = 1000;
        int consumerCount = 5;

        // 先发送消息
        for (int i = 0; i < messageCount; i++) {
            pgmqManager.queue().send(topic, "message-" + i);
        }

        List<Message> allReceivedMessages = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch latch = new CountDownLatch(consumerCount);

        // 并发消费消息
        for (int i = 0; i < consumerCount; i++) {
            executorService.submit(() -> {
                try {
                    List<Message> messages;
                    while (!(messages = pgmqManager.queue().poll(topic, 10)).isEmpty()) {
                        allReceivedMessages.addAll(messages);
                        // 删除消息
                        messages.forEach(Message::delete);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean completed = latch.await(30, TimeUnit.SECONDS);
        assertThat(completed).isTrue();

        // 验证所有消息都被消费
        assertThat(allReceivedMessages).hasSize(messageCount);

        // 验证没有重复消费
        long uniqueMessages = allReceivedMessages.stream()
                .map(Message::getPayload)
                .distinct()
                .count();
        assertThat(uniqueMessages).isEqualTo(messageCount);
    }

    @Test
    void shouldHandleConcurrentMessageProcessing() throws InterruptedException {
        String topic = "concurrent-processing-topic";
        int messageCount = 500;
        AtomicInteger processedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(messageCount);

        MessageHandler handler = new MessageHandler() {
            @Override
            public String topic() {
                return topic;
            }

            @Override
            public int threadCount() {
                return 8; // 使用8个线程并发处理
            }

            @Override
            public int maxPoll() {
                return 20; // 每次拉取20条消息
            }

            @Override
            public void handle(Message message) {
                try {
                    // 模拟处理时间
                    Thread.sleep(10);
                    processedCount.incrementAndGet();
                    message.delete();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            }
        };

        // 注册处理器
        pgmqManager.registerHandler(handler);

        // 发送消息
        for (int i = 0; i < messageCount; i++) {
            pgmqManager.queue().send(topic, "message-" + i);
        }

        // 等待所有消息处理完成
        boolean completed = latch.await(60, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
        assertThat(processedCount.get()).isEqualTo(messageCount);
    }

    @Test
    void shouldHandleConcurrentPriorityMessages() throws InterruptedException {
        String topic = "concurrent-priority-topic";
        int threadCount = 5;
        int messagesPerPriority = 50;
        CountDownLatch sendLatch = new CountDownLatch(threadCount);

        // 并发发送不同优先级的消息
        for (int i = 0; i < threadCount; i++) {
            final int priority = i + 1;
            executorService.submit(() -> {
                try {
                    for (int j = 0; j < messagesPerPriority; j++) {
                        pgmqManager.priorityQueue().send(topic, "priority-" + priority + "-message-" + j, priority);
                    }
                } finally {
                    sendLatch.countDown();
                }
            });
        }

        boolean sendCompleted = sendLatch.await(30, TimeUnit.SECONDS);
        assertThat(sendCompleted).isTrue();

        // 消费消息并验证优先级顺序
        List<Message> messages = pgmqManager.queue().poll(topic, 1000);
        assertThat(messages).hasSize(threadCount * messagesPerPriority);

        // 验证消息按优先级排序（高优先级在前）
        for (int i = 0; i < messages.size() - 1; i++) {
            assertThat(messages.get(i).getPriority()).isGreaterThanOrEqualTo(messages.get(i + 1).getPriority());
        }

        // 清理消息
        messages.forEach(Message::delete);
    }

    @Test
    void shouldHandleConcurrentRetryOperations() throws InterruptedException {
        String topic = "concurrent-retry-topic";
        int messageCount = 100;

        // 发送消息
        for (int i = 0; i < messageCount; i++) {
            pgmqManager.queue().send(topic, "retry-message-" + i);
        }

        // 获取所有消息
        List<Message> messages = pgmqManager.queue().poll(topic, messageCount);
        assertThat(messages).hasSize(messageCount);

        CountDownLatch retryLatch = new CountDownLatch(messageCount);

        // 并发重试消息
        for (Message message : messages) {
            executorService.submit(() -> {
                try {
                    message.retry();
                } finally {
                    retryLatch.countDown();
                }
            });
        }

        boolean retryCompleted = retryLatch.await(30, TimeUnit.SECONDS);
        assertThat(retryCompleted).isTrue();

        // 验证消息已重新进入待处理队列
        assertThat(countPendingMessages(topic)).isEqualTo(messageCount);
    }

    @Test
    void shouldHandleConcurrentDeadLetterOperations() throws InterruptedException {
        String topic = "concurrent-dead-topic";
        int messageCount = 100;

        // 发送消息
        for (int i = 0; i < messageCount; i++) {
            pgmqManager.queue().send(topic, "dead-message-" + i);
        }

        // 获取所有消息
        List<Message> messages = pgmqManager.queue().poll(topic, messageCount);
        assertThat(messages).hasSize(messageCount);

        CountDownLatch deadLatch = new CountDownLatch(messageCount);

        // 并发移动消息到死信队列
        for (Message message : messages) {
            executorService.submit(() -> {
                try {
                    message.dead();
                } finally {
                    deadLatch.countDown();
                }
            });
        }

        boolean deadCompleted = deadLatch.await(30, TimeUnit.SECONDS);
        assertThat(deadCompleted).isTrue();

        // 验证消息已移动到死信队列
        assertThat(countDeadMessages(topic)).isEqualTo(messageCount);
    }
}
