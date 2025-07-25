package github.luckygc.pgq.api;

public interface MessageProcessor {

    String getTopic();

    void onMessageAvailable(PgQueue pgQueue);
}
