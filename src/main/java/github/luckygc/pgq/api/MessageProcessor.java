package github.luckygc.pgq.api;

public interface MessageProcessor {

    PgQueue pgQueue();

    void onMessageAvailable();
}
