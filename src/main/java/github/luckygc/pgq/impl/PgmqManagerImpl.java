package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.DelayMessageQueue;
import github.luckygc.pgq.api.MessageProcessor;
import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.PriorityMessageQueue;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.dao.MessageDao;
import github.luckygc.pgq.dao.QueueDao;
import github.luckygc.pgq.model.PgmqConstants;
import github.luckygc.pgq.tool.MessageProcessorDispatcher;
import github.luckygc.pgq.tool.PgListener;
import github.luckygc.pgq.tool.PgNotifier;
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
    private final MessageProcessorDispatcher dispatcher;

    @Nullable
    private final PgNotifier pgNotifier;
    @Nullable
    private final PgListener pgListener;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public PgmqManagerImpl(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, null, null, null);
    }

    public PgmqManagerImpl(JdbcTemplate jdbcTemplate, String jdbcUrl, String username, String password) {
        this.queueDao = new QueueDao(jdbcTemplate);

        this.dispatcher = new MessageProcessorDispatcher();

        if (jdbcUrl == null) {
            this.pgNotifier = null;
            this.pgListener = null;
        } else {
            Objects.requireNonNull(username);
            this.pgNotifier = new PgNotifier(queueDao);
            this.pgListener = new PgListener(PgmqConstants.TOPIC_CHANNEL, jdbcUrl, username, password, dispatcher);
            try {
                this.pgListener.startListen();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        MessageDao messageDao = new MessageDao(jdbcTemplate);
        this.messageQueue = new MessageQueueImpl(messageDao, dispatcher, pgNotifier);

        scheduler.scheduleWithFixedDelay(this::schedule, 0, 1, TimeUnit.MINUTES);
    }

    private void schedule() {
        List<String> topics = queueDao.moveTimeoutAndVisibleMsgToPendingAndReturnPendingTopics();
        if (topics.isEmpty()) {
            return;
        }

        if (pgNotifier != null) {
            queueDao.sendNotify(topics);
        }

        for (String topic : topics) {
            dispatcher.dispatch(topic);
        }
    }

    @Override
    public void shutdown() {
        if (pgListener != null) {
            pgListener.stopListen();
        }

        scheduler.shutdownNow();
        dispatcher.shutdown();
        log.info("pgmq已停止");
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
        MessageProcessor messageProcessor = new MessageProcessor(messageQueue, messageHandler);
        dispatcher.register(messageProcessor);
    }

    @Override
    public void unregisterHandler(MessageHandler messageHandler) {

    }
}
