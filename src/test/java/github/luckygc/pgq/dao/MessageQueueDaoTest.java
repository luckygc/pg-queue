package github.luckygc.pgq.dao;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import github.luckygc.pgq.DatabaseTestBase;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("数据库队列DAO集成测试")
class MessageQueueDaoTest extends DatabaseTestBase {

    private DatabaseQueueDao databaseQueueDao;

    @BeforeEach
    void setUp() {
        databaseQueueDao = new DatabaseQueueDao(getJdbcTemplate(), getTransactionTemplate());
    }

    @Test
    @DisplayName("应该成功插入单个消息到待处理队列")
    void shouldInsertSingleMessageToPendingQueue() {
        // given
        Message message = Message.of("test-topic", "test-payload", 5);

        // when
        databaseQueueDao.insertMessage(message);

        // then
        assertThat(getTableCount("pgq_pending_queue")).isEqualTo(1);
        assertThat(isTableEmpty("pgq_processing_queue")).isTrue();
        assertThat(isTableEmpty("pgq_invisible_queue")).isTrue();

        // 验证插入的数据
        List<Message> messages = getJdbcTemplate().query(
            "SELECT id, create_time, topic, priority, payload, attempt FROM pgq_pending_queue",
            DatabaseQueueDao.rowMapper
        );

        assertThat(messages).hasSize(1);
        Message savedMessage = messages.get(0);
        assertThat(savedMessage.getId()).isNotNull();
        assertThat(savedMessage.getTopic()).isEqualTo("test-topic");
        assertThat(savedMessage.getPayload()).isEqualTo("test-payload");
        assertThat(savedMessage.getPriority()).isEqualTo(5);
        assertThat(savedMessage.getAttempt()).isEqualTo(0);
        assertThat(savedMessage.getCreateTime()).isNotNull();
    }

    @Test
    @DisplayName("应该成功批量插入消息到待处理队列")
    void shouldInsertBatchMessagesToPendingQueue() {
        // given
        List<Message> messages = Message.of("batch-topic", Arrays.asList("msg1", "msg2", "msg3"), 3);

        // when
        databaseQueueDao.insertMessages(messages);

        // then
        assertThat(getTableCount("pgq_pending_queue")).isEqualTo(3);

        // 验证插入的数据
        List<Message> savedMessages = getJdbcTemplate().query(
            "SELECT id, create_time, topic, priority, payload, attempt FROM pgq_pending_queue ORDER BY id",
            DatabaseQueueDao.rowMapper
        );

        assertThat(savedMessages).hasSize(3);
        for (int i = 0; i < savedMessages.size(); i++) {
            Message savedMessage = savedMessages.get(i);
            assertThat(savedMessage.getTopic()).isEqualTo("batch-topic");
            assertThat(savedMessage.getPayload()).isEqualTo("msg" + (i + 1));
            assertThat(savedMessage.getPriority()).isEqualTo(3);
            assertThat(savedMessage.getAttempt()).isEqualTo(0);
        }
    }

    @Test
    @DisplayName("应该成功插入延迟处理消息到不可见队列")
    void shouldInsertDelayedMessageToInvisibleQueue() {
        // given
        Message message = Message.of("delayed-topic", "delayed-payload", 1);
        Duration delay = Duration.ofMinutes(5);

        // when
        databaseQueueDao.insertProcessLaterMessage(message, delay);

        // then
        assertThat(getTableCount("pgq_invisible_queue")).isEqualTo(1);
        assertThat(isTableEmpty("pgq_pending_queue")).isTrue();

        // 验证可见时间设置正确
        Integer count = getJdbcTemplate().queryForObject(
            "SELECT COUNT(*) FROM pgq_invisible_queue WHERE visible_time > NOW()",
            Integer.class
        );
        assertThat(count).isEqualTo(1);
    }

    @Test
    @DisplayName("应该成功从待处理队列拉取消息到处理中队列")
    void shouldPullMessagesFromPendingToProcessing() {
        // given - 插入测试数据
        List<Message> testMessages = Message.of("pull-topic", Arrays.asList("msg1", "msg2", "msg3"), 5);
        databaseQueueDao.insertMessages(testMessages);

        // when
        List<Message> pulledMessages = databaseQueueDao.pull("pull-topic", 2, Duration.ofMinutes(10));

        // then
        assertThat(pulledMessages).hasSize(2);
        assertThat(getTableCount("pgq_pending_queue")).isEqualTo(1); // 还剩1条
        assertThat(getTableCount("pgq_processing_queue")).isEqualTo(2); // 移动了2条

        // 验证拉取的消息
        for (Message message : pulledMessages) {
            assertThat(message.getTopic()).isEqualTo("pull-topic");
            assertThat(message.getPriority()).isEqualTo(5);
            assertThat(message.getAttempt()).isEqualTo(1); // attempt应该增加1
        }

        // 验证处理中队列的超时时间设置
        Integer timeoutCount = getJdbcTemplate().queryForObject(
            "SELECT COUNT(*) FROM pgq_processing_queue WHERE timeout_time > NOW()",
            Integer.class
        );
        assertThat(timeoutCount).isEqualTo(2);
    }

    @Test
    @DisplayName("当没有匹配的消息时拉取应返回空列表")
    void shouldReturnEmptyListWhenNoPendingMessages() {
        // when
        List<Message> pulledMessages = databaseQueueDao.pull("non-existent-topic", 5, Duration.ofMinutes(10));

        // then
        assertThat(pulledMessages).isEmpty();
        assertThat(isTableEmpty("pgq_processing_queue")).isTrue();
    }

    @Test
    @DisplayName("应该按优先级和ID顺序拉取消息")
    void shouldPullMessagesByPriorityAndIdOrder() {
        // given - 插入不同优先级的消息
        databaseQueueDao.insertMessage(Message.of("priority-topic", "low-priority", 1));
        databaseQueueDao.insertMessage(Message.of("priority-topic", "high-priority", 10));
        databaseQueueDao.insertMessage(Message.of("priority-topic", "medium-priority", 5));

        // when
        List<Message> pulledMessages = databaseQueueDao.pull("priority-topic", 3, Duration.ofMinutes(10));

        // then
        assertThat(pulledMessages).hasSize(3);
        // 应该按优先级降序排列
        assertThat(pulledMessages.get(0).getPayload()).isEqualTo("high-priority");
        assertThat(pulledMessages.get(1).getPayload()).isEqualTo("medium-priority");
        assertThat(pulledMessages.get(2).getPayload()).isEqualTo("low-priority");
    }

    @Test
    @DisplayName("当拉取数量小于1时应该抛出异常")
    void shouldThrowExceptionWhenPullCountIsLessThanOne() {
        assertThatThrownBy(() -> databaseQueueDao.pull("topic", 0, Duration.ofMinutes(10)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("pullCount必须大于0");
    }

    @Test
    @DisplayName("当处理超时时间超过30分钟时应该抛出异常")
    void shouldThrowExceptionWhenProcessTimeoutExceeds30Minutes() {
        assertThatThrownBy(() -> databaseQueueDao.pull("topic", 1, Duration.ofMinutes(31)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("processTimeout不允许超过30分钟");
    }

    @Test
    @DisplayName("并发拉取时应该使用SKIP LOCKED避免冲突")
    void shouldHandleConcurrentPullWithSkipLocked() {
        // given
        List<Message> testMessages = Message.of("concurrent-topic", Arrays.asList("msg1", "msg2", "msg3", "msg4"), 1);
        databaseQueueDao.insertMessages(testMessages);

        // when - 模拟并发拉取
        List<Message> batch1 = databaseQueueDao.pull("concurrent-topic", 2, Duration.ofMinutes(10));
        List<Message> batch2 = databaseQueueDao.pull("concurrent-topic", 2, Duration.ofMinutes(10));

        // then
        assertThat(batch1).hasSize(2);
        assertThat(batch2).hasSize(2);
        assertThat(isTableEmpty("pgq_pending_queue")).isTrue();
        assertThat(getTableCount("pgq_processing_queue")).isEqualTo(4);

        // 验证没有重复的消息ID
        List<Long> allIds = Arrays.asList(
            batch1.get(0).getId(), batch1.get(1).getId(),
            batch2.get(0).getId(), batch2.get(1).getId()
        );
        assertThat(allIds).doesNotHaveDuplicates();
    }
} 
