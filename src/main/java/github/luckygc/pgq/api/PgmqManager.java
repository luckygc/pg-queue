package github.luckygc.pgq.api;

import github.luckygc.pgq.api.handler.MessageHandler;
import java.sql.SQLException;

public interface PgmqManager {

    MessageQueue queue();

    DelayMessageQueue delayQueue();

    PriorityMessageQueue priorityQueue();

    void registerHandler(MessageHandler messageHandler);

    void unregisterHandler(MessageHandler messageHandler);

    /**
     * \
     *
     * @throws SQLException 开启PgNotify功能时启动可能会抛出
     */
    void start() throws SQLException;

    void stop();
}
