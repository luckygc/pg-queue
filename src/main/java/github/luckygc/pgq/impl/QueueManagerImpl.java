package github.luckygc.pgq.impl;

import github.luckygc.pgq.ListenerDispatcher;
import github.luckygc.pgq.PgNotifyListener;
import github.luckygc.pgq.PgqConstants;
import github.luckygc.pgq.QueueDao;
import github.luckygc.pgq.api.BatchMessageHandler;
import github.luckygc.pgq.api.DatabaseQueue;
import github.luckygc.pgq.api.DeadMessageManger;
import github.luckygc.pgq.api.MessageManager;
import github.luckygc.pgq.api.QueueListener;
import github.luckygc.pgq.api.QueueManager;
import github.luckygc.pgq.api.SingleMessageHandler;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class QueueManagerImpl implements QueueManager {

    private static final Logger log = LoggerFactory.getLogger(QueueManagerImpl.class);

    private final Map<String, DatabaseQueue> queueMap = new ConcurrentHashMap<>();
    private final Map<String, QueueListener> listenerMap = new ConcurrentHashMap<>();

    private final ListenerDispatcher listenerDispatcher;
    private final boolean isEnablePgNotify;
    private PgNotifyListener pgNotifyListener;
    private final QueueDao queueDao;
    private final MessageManager messageManager;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public QueueManagerImpl(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.listenerDispatcher = new ListenerDispatcher(this);
        this.queueDao = new QueueDao(jdbcTemplate, transactionTemplate, listenerDispatcher, false);
        this.messageManager = new MessageManagerImpl(queueDao);
        this.isEnablePgNotify = false;
    }

    public QueueManagerImpl(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate, String jdbcUrl,
            String username, String password) {
        this.listenerDispatcher = new ListenerDispatcher(this);
        this.queueDao = new QueueDao(jdbcTemplate, transactionTemplate, listenerDispatcher, true);
        this.messageManager = new MessageManagerImpl(queueDao);
        this.isEnablePgNotify = true;
        this.pgNotifyListener = new PgNotifyListener(PgqConstants.TOPIC_CHANNEL, jdbcUrl, username, password,
                listenerDispatcher);
    }

    @Override
    public DatabaseQueue queue(String topic) {
        return queueMap.compute(topic, (k, v) -> {
            if (v != null) {
                return v;
            }

            return new DatabaseQueueImpl(queueDao, k, listenerDispatcher);
        });
    }

    @Override
    public void registerListener(QueueListener messageListener) {
        QueueListener queueListener = listenerMap.putIfAbsent(messageListener.topic(), messageListener);
        if (queueListener != null) {
            throw new IllegalStateException("当前已存在topic[{}]的监听器");
        }
    }

    @Override
    public void registerMessageHandler(SingleMessageHandler messageHandler) {
        DatabaseQueue queue = queue(messageHandler.topic());
        SingleMessageProcessor processor = new SingleMessageProcessor(queue, messageManager, messageHandler);
        registerListener(processor);
    }

    @Override
    public void registerMessageHandler(BatchMessageHandler messageHandler) {
        DatabaseQueue queue = queue(messageHandler.topic());
        BatchMessageProcessor processor = new BatchMessageProcessor(queue, messageManager, messageHandler);
        registerListener(processor);
    }

    @Override
    public @Nullable QueueListener listener(String topic) {
        return listenerMap.get(topic);
    }

    @Override
    public MessageManager messageManager() {
        return messageManager;
    }

    @Override
    public DeadMessageManger deadMessageManager() {
        return null;
    }

    @Override
    public boolean isEnablePgNotify() {
        return false;
    }

    @Override
    public void start() throws SQLException {
        pgNotifyListener.start();
    }

    @Override
    public void stop() {
        pgNotifyListener.stop();
    }
}
