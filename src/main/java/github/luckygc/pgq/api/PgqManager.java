package github.luckygc.pgq.api;

public interface PgqManager {

    PgQueue registerQueue(String topic);

    PgQueue registerMessageHandler(MessageHandler... handlers);

    MessageManager messageManager();
}
