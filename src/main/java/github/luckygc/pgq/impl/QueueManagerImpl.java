package github.luckygc.pgq.impl;

import github.luckygc.pgq.MessageProcessor;
import github.luckygc.pgq.MessageAvailableCallbackDispatcher;
import github.luckygc.pgq.PgChannelListener;
import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.SingleMessageAvailableCallBackImpl;
import github.luckygc.pgq.api.handler.BatchMessageHandler;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.manager.DeadMessageManager;
import github.luckygc.pgq.api.manager.MessageManager;
import github.luckygc.pgq.api.manager.QueueManager;
import github.luckygc.pgq.api.handler.SingleMessageHandler;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.dao.QueueManagerDao;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueManagerImpl implements QueueManager {

    private static final Logger log = LoggerFactory.getLogger(QueueManagerImpl.class);

    private final Map<String, MessageQueue> queueMap = new ConcurrentHashMap<>();

    private final MessageAvailableCallbackDispatcher messageAvailableCallbackDispatcher;
    private final QueueManagerDao queueManagerDao;
    private final MessageDao messageDao;
    private final MessageManager messageManager;
    private final DeadMessageManager deadMessageManager;
    private ScheduledExecutorService scheduler;

    private final boolean enablePgNotify;
    private PgChannelListener pgChannelListener;

    public QueueManagerImpl(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.messageAvailableCallbackDispatcher = new MessageAvailableCallbackDispatcher();
        this.queueManagerDao = new QueueManagerDao(jdbcTemplate, transactionTemplate);
        this.messageDao = new MessageDao(jdbcTemplate, transactionTemplate);
        MessageDao messageDao = new MessageDao(jdbcTemplate);
        this.messageManager = new MessageManagerImpl(messageDao);
        this.deadMessageManager = new DeadMessageManagerImpl(messageDao);
        this.enablePgNotify = false;
    }

    public QueueManagerImpl(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate, String jdbcUrl,
            String username, String password) {
        this.messageAvailableCallbackDispatcher = new MessageAvailableCallbackDispatcher();
        this.queueManagerDao = new QueueManagerDao(jdbcTemplate, transactionTemplate);
        this.messageDao = new MessageDao(jdbcTemplate, transactionTemplate);
        MessageDao messageDao = new MessageDao(jdbcTemplate);
        this.messageManager = new MessageManagerImpl(messageDao);
        this.deadMessageManager = new DeadMessageManagerImpl(messageDao);
        this.enablePgNotify = true;
        this.pgChannelListener = new PgChannelListener(PgmqConstants.TOPIC_CHANNEL, Objects.requireNonNull(jdbcUrl),
                Objects.requireNonNull(username), password, messageAvailableCallbackDispatcher);
    }

    @Override
    public MessageQueue queue(String topic) {
        return queueMap.compute(topic, (k, v) -> {
            if (v != null) {
                return v;
            }

            if (enablePgNotify) {
                return new MessageQueueImpl(messageDao, k, messageAvailableCallbackDispatcher, queueManagerDao);
            }

            return new MessageQueueImpl(messageDao, k, messageAvailableCallbackDispatcher);
        });
    }

    @Override
    public void registerMessageHandler(SingleMessageHandler messageHandler) {
        MessageQueue queue = queue(messageHandler.topic());
        SingleMessageAvailableCallBackImpl processor = new SingleMessageAvailableCallBackImpl(queue, messageManager, messageHandler);
        messageAvailableCallbackDispatcher.register(processor);
    }

    @Override
    public void registerMessageHandler(BatchMessageHandler messageHandler) {
        MessageQueue queue = queue(messageHandler.topic());
        MessageProcessor processor = new MessageProcessor(queue, messageManager, messageHandler);
        messageAvailableCallbackDispatcher.register(processor);
    }

    @Override
    public MessageManager processingMessageManager() {
        return messageManager;
    }

    @Override
    public DeadMessageManager deadMessageManager() {
        return deadMessageManager;
    }

    @Override
    public boolean isEnablePgNotify() {
        return enablePgNotify;
    }

    @Override
    public void start() throws SQLException {
        if (enablePgNotify) {
            pgChannelListener.startListen();
        }

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(this::schedule, 0, 1, TimeUnit.MINUTES);

        log.debug("启动pgq成功");
    }

    private void schedule() {
        List<String> topics = queueManagerDao.tryHandleTimeoutAndVisibleMessagesAndReturnTopicsWithAvailableMessages();
        if (topics.isEmpty()) {
            return;
        }

        for (String topic : topics) {
            messageAvailableCallbackDispatcher.dispatch(topic);
            if (enablePgNotify) {
                queueManagerDao.sendNotify(topic);
            }
        }
    }

    @Override
    public void stop() {
        if (enablePgNotify) {
            pgChannelListener.stopListen();
        }

        scheduler.shutdownNow();
        scheduler = null;

        // 关闭所有消息处理器的线程池
        messageAvailableCallbackDispatcher.shutdown();

        log.debug("停止pgq成功");
    }

    @Override
    public Map<String, String> getThreadPoolStatus() {
        return messageAvailableCallbackDispatcher.getThreadPoolStatus();
    }
}
