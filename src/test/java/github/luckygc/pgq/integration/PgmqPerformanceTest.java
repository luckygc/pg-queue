package github.luckygc.pgq.integration;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.impl.PgmqManagerImpl;
import github.luckygc.pgq.model.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class PgmqPerformanceTest extends BaseIntegrationTest {

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
    void shouldHandleHighThroughputMessageSending() {
        String topic = "performance-send-topic";
        int messageCount = 10000;
        
        Instant start = Instant.now();
        
        // 批量发送消息
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            messages.add("performance-message-" + i);
        }
        
        // 分批发送以提高性能
        int batchSize = 1000;
        for (int i = 0; i < messageCount; i += batchSize) {
            int endIndex = Math.min(i + batchSize, messageCount);
            pgmqManager.queue().send(topic, messages.subList(i, endIndex));
        }
        
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        
        System.out.println("发送 " + messageCount + " 条消息耗时: " + duration.toMillis() + "ms");
        System.out.println("平均每秒发送: " + (messageCount * 1000.0 / duration.toMillis()) + " 条消息");
        
        // 验证所有消息都已发送
        assertThat(countPendingMessages(topic)).isEqualTo(messageCount);
        
        // 性能断言：应该能在10秒内发送完成
        assertThat(duration.getSeconds()).isLessThan(10);
    }

    @Test
    void shouldHandleHighThroughputMessagePolling() {
        String topic = "performance-poll-topic";
        int messageCount = 10000;
        
        // 先发送消息
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < messageCount; i++) {
            messages.add("poll-message-" + i);
        }
        pgmqManager.queue().send(topic, messages);
        
        Instant start = Instant.now();
        
        List<Message> allMessages = new ArrayList<>();
        List<Message> batch;
        while (!(batch = pgmqManager.queue().poll(topic, 1000)).isEmpty()) {
            allMessages.addAll(batch);
        }
        
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        
        System.out.println("拉取 " + allMessages.size() + " 条消息耗时: " + duration.toMillis() + "ms");
        System.out.println("平均每秒拉取: " + (allMessages.size() * 1000.0 / duration.toMillis()) + " 条消息");
        
        assertThat(allMessages).hasSize(messageCount);
        
        // 性能断言：应该能在5秒内拉取完成
        assertThat(duration.getSeconds()).isLessThan(5);
    }

    @Test
    void shouldHandleLargeMessagePayloads() {
        String topic = "large-payload-topic";
        int messageCount = 1000;
        
        // 创建大的消息负载（约10KB）
        StringBuilder largePayload = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            largePayload.append("x");
        }
        String payload = largePayload.toString();
        
        Instant start = Instant.now();
        
        // 发送大消息
        for (int i = 0; i < messageCount; i++) {
            pgmqManager.queue().send(topic, payload + "-" + i);
        }
        
        Instant end = Instant.now();
        Duration sendDuration = Duration.between(start, end);
        
        System.out.println("发送 " + messageCount + " 条大消息(10KB)耗时: " + sendDuration.toMillis() + "ms");
        
        // 拉取消息
        start = Instant.now();
        List<Message> messages = pgmqManager.queue().poll(topic, messageCount);
        end = Instant.now();
        Duration pollDuration = Duration.between(start, end);
        
        System.out.println("拉取 " + messages.size() + " 条大消息耗时: " + pollDuration.toMillis() + "ms");
        
        assertThat(messages).hasSize(messageCount);
        
        // 验证消息内容
        for (Message message : messages) {
            assertThat(message.getPayload()).startsWith(payload);
        }
        
        // 清理消息
        messages.forEach(Message::delete);
        
        // 性能断言：大消息处理应该在合理时间内完成
        assertThat(sendDuration.getSeconds()).isLessThan(15);
        assertThat(pollDuration.getSeconds()).isLessThan(10);
    }

    @Test
    void shouldHandleMultipleTopicsPerformance() {
        int topicCount = 100;
        int messagesPerTopic = 100;
        
        Instant start = Instant.now();
        
        // 向多个topic发送消息
        for (int i = 0; i < topicCount; i++) {
            String topic = "multi-topic-" + i;
            List<String> messages = new ArrayList<>();
            for (int j = 0; j < messagesPerTopic; j++) {
                messages.add("message-" + j);
            }
            pgmqManager.queue().send(topic, messages);
        }
        
        Instant end = Instant.now();
        Duration duration = Duration.between(start, end);
        
        int totalMessages = topicCount * messagesPerTopic;
        System.out.println("向 " + topicCount + " 个topic发送 " + totalMessages + " 条消息耗时: " + duration.toMillis() + "ms");
        System.out.println("平均每秒发送: " + (totalMessages * 1000.0 / duration.toMillis()) + " 条消息");
        
        // 验证所有消息都已发送
        Integer totalCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_pending_queue",
                Integer.class
        );
        assertThat(totalCount).isEqualTo(totalMessages);
        
        // 性能断言
        assertThat(duration.getSeconds()).isLessThan(20);
    }
}
