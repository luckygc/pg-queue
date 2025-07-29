package github.luckygc.pgq.dao;

import static org.assertj.core.api.Assertions.assertThat;

import github.luckygc.pgq.integration.BaseIntegrationTest;
import java.time.LocalDateTime;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class QueueDaoTest extends BaseIntegrationTest {

    private QueueDao queueDao;

    @BeforeEach
    void setUp() {
        queueDao = new QueueDao(jdbcTemplate);
    }

    @Test
    void shouldReturnEmptyListWhenNoMessagesToMove() {
        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();

        assertThat(topics).isEmpty();
    }

    @Test
    void shouldMoveTimeoutProcessingMessagesToPending() {
        // 插入一条超时的处理中消息
        LocalDateTime timeoutTime = LocalDateTime.now().minusMinutes(1); // 1分钟前超时
        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "timeout-topic", 0, "timeout message", 0, timeoutTime
        );

        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();

        assertThat(topics).contains("timeout-topic");

        // 验证消息已从处理队列移除
        Integer processingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_processing_queue WHERE id = ?",
                Integer.class,
                1L
        );
        assertThat(processingCount).isEqualTo(0);

        // 验证消息已移动到待处理队列
        Integer pendingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_pending_queue WHERE id = ?",
                Integer.class,
                1L
        );
        assertThat(pendingCount).isEqualTo(1);
    }

    @Test
    void shouldMoveVisibleInvisibleMessagesToPending() {
        // 插入一条已到可见时间的不可见消息
        LocalDateTime visibleTime = LocalDateTime.now().minusMinutes(1); // 1分钟前就应该可见
        jdbcTemplate.update("""
                        INSERT INTO pgmq_invisible_queue
                            (id, create_time, topic, priority, payload, attempt, visible_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                2L, LocalDateTime.now(), "visible-topic", 0, "visible message", 0, visibleTime
        );

        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();

        assertThat(topics).contains("visible-topic");

        // 验证消息已从不可见队列移除
        Integer invisibleCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_invisible_queue WHERE id = ?",
                Integer.class,
                2L
        );
        assertThat(invisibleCount).isEqualTo(0);

        // 验证消息已移动到待处理队列
        Integer pendingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_pending_queue WHERE id = ?",
                Integer.class,
                2L
        );
        assertThat(pendingCount).isEqualTo(1);
    }

    @Test
    void shouldHandleMultipleTopics() {
        // 插入不同topic的消息
        LocalDateTime timeoutTime = LocalDateTime.now().minusMinutes(1);
        LocalDateTime visibleTime = LocalDateTime.now().minusMinutes(1);

        // 超时的处理中消息
        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "topic1", 0, "message1", 0, timeoutTime
        );

        // 可见的不可见消息
        jdbcTemplate.update("""
                        INSERT INTO pgmq_invisible_queue
                            (id, create_time, topic, priority, payload, attempt, visible_time) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                2L, LocalDateTime.now(), "topic2", 0, "message2", 0, visibleTime
        );

        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();

        assertThat(topics).containsExactlyInAnyOrder("topic1", "topic2");
    }

    @Test
    void shouldNotMoveNonTimeoutProcessingMessages() {
        // 插入一条未超时的处理中消息
        LocalDateTime futureTimeoutTime = LocalDateTime.now().plusMinutes(30);
        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "future-topic", 0, "future message", 0, futureTimeoutTime
        );

        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();

        // 不应该包含这个topic，因为消息还没超时
        assertThat(topics).doesNotContain("future-topic");

        // 验证消息仍在处理队列中
        Integer processingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_processing_queue WHERE id = ?",
                Integer.class,
                1L
        );
        assertThat(processingCount).isEqualTo(1);
    }

    @Test
    void shouldNotMoveNonVisibleInvisibleMessages() {
        // 插入一条未到可见时间的不可见消息
        LocalDateTime futureVisibleTime = LocalDateTime.now().plusMinutes(30);
        jdbcTemplate.update("""
                        INSERT INTO pgmq_invisible_queue
                            (id, create_time, topic, priority, payload, attempt, visible_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                2L, LocalDateTime.now(), "future-visible-topic", 0, "future visible message", 0, futureVisibleTime
        );

        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();

        // 不应该包含这个topic，因为消息还不可见
        assertThat(topics).doesNotContain("future-visible-topic");

        // 验证消息仍在不可见队列中
        Integer invisibleCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_invisible_queue WHERE id = ?",
                Integer.class,
                2L
        );
        assertThat(invisibleCount).isEqualTo(1);
    }

    @Test
    void shouldHandleConcurrentExecution() {
        // 这个测试验证咨询锁的功能
        // 由于咨询锁的特性，同时只能有一个事务执行这个函数

        // 插入一条超时消息
        LocalDateTime timeoutTime = LocalDateTime.now().minusMinutes(1);
        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "concurrent-topic", 0, "concurrent message", 0, timeoutTime
        );

        // 第一次调用应该成功
        List<String> topics1 = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();
        assertThat(topics1).contains("concurrent-topic");

        // 第二次调用应该返回空列表，因为消息已经被移动
        List<String> topics2 = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();
        assertThat(topics2).doesNotContain("concurrent-topic");
    }

    @Test
    void shouldReturnDistinctTopics() {
        // 插入同一个topic的多条消息
        LocalDateTime timeoutTime = LocalDateTime.now().minusMinutes(1);

        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "same-topic", 0, "message1", 0, timeoutTime
        );

        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                2L, LocalDateTime.now(), "same-topic", 0, "message2", 0, timeoutTime
        );

        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();

        // 应该只返回一次topic名称，即使有多条消息
        assertThat(topics).containsExactly("same-topic");
    }
}
