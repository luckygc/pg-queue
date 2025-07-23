package github.luckygc.pgq.api;

public interface PgqManager {

    PgQueue queue(String topic);

    MessageManager messageManager();
}
