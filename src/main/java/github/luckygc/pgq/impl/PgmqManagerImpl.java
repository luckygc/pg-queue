package github.luckygc.pgq.impl;

import github.luckygc.pgq.AsyncMessageProcessor;
import github.luckygc.pgq.MessageProcessorDispatcher;
import github.luckygc.pgq.PgListener;
import github.luckygc.pgq.PgNotifier;
import github.luckygc.pgq.PgmqConstants;
import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.PriorityMessageQueue;
import github.luckygc.pgq.api.callback.MessageAvailableCallback;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.dao.QueueDao;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class PgmqManagerImpl implements PgmqManager {

    private static final Logger log = LoggerFactory.getLogger(PgmqManagerImpl.class);

    private final QueueDao queueDao;
    private final MessageQueueImpl messageQueue;

    @Nullable
    private final PgNotifier pgNotifier;
    @Nullable
    private final PgListener pgListener;

    private ScheduledExecutorService scheduler;

    public PgmqManagerImpl(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, null, null, null);
    }

    public PgmqManagerImpl(JdbcTemplate jdbcTemplate, String jdbcUrl, String username, String password) {
        this.queueDao = new QueueDao(jdbcTemplate);


        MessageAvailableCallback callback = new MessageProcessorDispatcher();

        if (jdbcUrl == null) {
            this.pgNotifier = null;
            this.pgListener = null;
        } else {
            Objects.requireNonNull(username);
            this.pgNotifier = new PgNotifier(queueDao);
            this.pgListener = new PgListener(PgmqConstants.TOPIC_CHANNEL, jdbcUrl, username, password, callbacks);
        }

        MessageDao messageDao = new MessageDao(jdbcTemplate);
        this.messageQueue = new MessageQueueImpl(messageDao, callback, pgNotifier);
    }

    @Override
    public void start() throws SQLException {
        if (pgListener != null) {
            pgListener.startListen();
        }

        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleWithFixedDelay(this::schedule, 0, 1, TimeUnit.MINUTES);

        log.debug("启动pgq成功");
    }

    private void schedule() {
        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();
        if (topics.isEmpty()) {
            return;
        }

        if (pgNotifier != null) {
            pgNotifier.sendNotify(topics);
        }
    }

    @Override
    public void stop() {
        if (pgListener != null) {
            pgListener.stopListen();
        }

        scheduler.shutdownNow();
        scheduler = null;

        log.debug("停止pgq成功");
    }

    @Override
    public MessageQueue queue() {
        return messageQueue;
    }

    @Override
    public DelayMessageQueue delayQueue() {
        return messageQueue;
    }

    @Override
    public PriorityMessageQueue priorityQueue() {
        return messageQueue;
    }

    @Override
    public void registerHandler(MessageHandler messageHandler) {
        AsyncMessageProcessor asyncMessageProcessor = new AsyncMessageProcessor(messageQueue, messageHandler);
        messageProcessorDispatcher.register(asyncMessageProcessor);
    }

    @Override
    public void unregisterHandler(MessageHandler messageHandler) {

    }
}
