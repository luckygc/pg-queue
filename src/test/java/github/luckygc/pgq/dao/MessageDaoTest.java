package github.luckygc.pgq.dao;

import static org.assertj.core.api.Assertions.assertThat;

import github.luckygc.pgq.integration.BaseIntegrationTest;
import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.model.MessageDO;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@DisplayName("消息DAO测试")
class MessageDaoTest extends BaseIntegrationTest {

    private MessageDao messageDao;

    @BeforeEach
    void setUp() {
        messageDao = new MessageDao(jdbcTemplate);
    }

    @Test
    @DisplayName("应该能够插入单条消息到待处理队列")
    void shouldInsertMessageIntoPending() {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(5)
                .payload("test payload")
                .attempt(0)
                .build();

        messageDao.insertIntoPending(messageDO);

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_pending_queue WHERE topic = ?",
                Integer.class,
                "test-topic"
        );
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("应该能够批量插入多条消息到待处理队列")
    void shouldInsertMultipleMessagesIntoPending() {
        List<MessageDO> messageDOs = Arrays.asList(
                MessageDO.Builder.create()
                        .topic("test-topic")
                        .priority(1)
                        .payload("message1")
                        .attempt(0)
                        .build(),
                MessageDO.Builder.create()
                        .topic("test-topic")
                        .priority(2)
                        .payload("message2")
                        .attempt(0)
                        .build()
        );

        messageDao.insertIntoPending(messageDOs);

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_pending_queue WHERE topic = ?",
                Integer.class,
                "test-topic"
        );
        assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("应该能够插入消息到不可见队列")
    void shouldInsertMessageIntoInvisible() {
        MessageDO messageDO = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(5)
                .payload("test payload")
                .attempt(0)
                .build();

        LocalDateTime visibleTime = LocalDateTime.now().plusMinutes(5);
        messageDao.insertIntoInvisible(messageDO, visibleTime);

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_invisible_queue WHERE topic = ?",
                Integer.class,
                "test-topic"
        );
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("应该能够获取待处理消息并移动到处理队列")
    void shouldGetPendingMessagesAndMoveToProcessing() {
        // 先插入一些待处理消息
        MessageDO messageDO1 = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(10)
                .payload("high priority")
                .attempt(0)
                .build();

        MessageDO messageDO2 = MessageDO.Builder.create()
                .topic("test-topic")
                .priority(5)
                .payload("low priority")
                .attempt(0)
                .build();

        messageDao.insertIntoPending(Arrays.asList(messageDO1, messageDO2));

        // 获取消息并移动到处理队列
        LocalDateTime timeoutTime = LocalDateTime.now().plusMinutes(30);
        List<Message> messages = messageDao.getPendingMessagesAndMoveToProcessing("test-topic", 10, timeoutTime);

        assertThat(messages).hasSize(2);
        // 应该按优先级排序，高优先级在前
        assertThat(messages.get(0).getPayload()).isEqualTo("high priority");
        assertThat(messages.get(0).getPriority()).isEqualTo(10);
        assertThat(messages.get(1).getPayload()).isEqualTo("low priority");
        assertThat(messages.get(1).getPriority()).isEqualTo(5);

        // 验证消息已从待处理队列移除
        Integer pendingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_pending_queue WHERE topic = ?",
                Integer.class,
                "test-topic"
        );
        assertThat(pendingCount).isEqualTo(0);

        // 验证消息已移动到处理队列
        Integer processingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_processing_queue WHERE topic = ?",
                Integer.class,
                "test-topic"
        );
        assertThat(processingCount).isEqualTo(2);
    }

    @Test
    @DisplayName("应该能够限制获取消息的数量")
    void shouldLimitNumberOfMessagesRetrieved() {
        // 插入5条消息
        for (int i = 0; i < 5; i++) {
            MessageDO messageDO = MessageDO.Builder.create()
                    .topic("test-topic")
                    .priority(i)
                    .payload("message" + i)
                    .attempt(0)
                    .build();
            messageDao.insertIntoPending(messageDO);
        }

        // 只获取3条消息
        LocalDateTime timeoutTime = LocalDateTime.now().plusMinutes(30);
        List<Message> messages = messageDao.getPendingMessagesAndMoveToProcessing("test-topic", 3, timeoutTime);

        assertThat(messages).hasSize(3);

        // 验证还有2条消息在待处理队列
        Integer pendingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_pending_queue WHERE topic = ?",
                Integer.class,
                "test-topic"
        );
        assertThat(pendingCount).isEqualTo(2);
    }

    @Test
    @DisplayName("应该能够根据ID删除处理队列中的消息")
    void shouldDeleteProcessingMessageById() {
        // 先插入一条消息到处理队列
        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "test-topic", 0, "test payload", 0, LocalDateTime.now().plusMinutes(30)
        );

        int deleteCount = messageDao.deleteProcessingMessageById(1L);

        assertThat(deleteCount).isEqualTo(1);

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_processing_queue WHERE id = ?",
                Integer.class,
                1L
        );
        assertThat(count).isEqualTo(0);
    }

    @Test
    @DisplayName("应该能够将处理队列中的消息移动到死信队列")
    void shouldMoveProcessingMessageToDeadById() {
        // 先插入一条消息到处理队列
        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "test-topic", 0, "test payload", 0, LocalDateTime.now().plusMinutes(30)
        );

        int moveCount = messageDao.moveProcessingMessageToDeadById(1L);

        assertThat(moveCount).isEqualTo(1);

        // 验证消息已从处理队列移除
        Integer processingCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_processing_queue WHERE id = ?",
                Integer.class,
                1L
        );
        assertThat(processingCount).isEqualTo(0);

        // 验证消息已移动到死信队列
        Integer deadCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM pgmq_dead_queue WHERE id = ?",
                Integer.class,
                1L
        );
        assertThat(deadCount).isEqualTo(1);
    }

    @Test
    @DisplayName("应该能够将处理队列中的消息移动到待处理队列")
    void shouldMoveProcessingMessageToPendingById() {
        // 先插入一条消息到处理队列
        jdbcTemplate.update("""
                        INSERT INTO pgmq_processing_queue
                            (id, create_time, topic, priority, payload, attempt, timeout_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                1L, LocalDateTime.now(), "test-topic", 0, "test payload", 0, LocalDateTime.now().plusMinutes(30)
        );

        int moveCount = messageDao.moveProcessingMessageToPendingById(1L);

        assertThat(moveCount).isEqualTo(1);

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
    @DisplayName("删除不存在的消息时应该返回0")
    void shouldReturnZeroWhenDeletingNonExistentMessage() {
        int deleteCount = messageDao.deleteProcessingMessageById(999L);
        assertThat(deleteCount).isEqualTo(0);
    }

    @Test
    @DisplayName("移动不存在的消息到死信队列时应该返回0")
    void shouldReturnZeroWhenMovingNonExistentMessageToDead() {
        int moveCount = messageDao.moveProcessingMessageToDeadById(999L);
        assertThat(moveCount).isEqualTo(0);
    }

    @Test
    @DisplayName("移动不存在的消息到待处理队列时应该返回0")
    void shouldReturnZeroWhenMovingNonExistentMessageToPending() {
        int moveCount = messageDao.moveProcessingMessageToPendingById(999L);
        assertThat(moveCount).isEqualTo(0);
    }
}
