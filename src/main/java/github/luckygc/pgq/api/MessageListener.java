package github.luckygc.pgq.api;

public interface MessageListener {

    String topic();

    void onMessageAvailable();

    void processMessages();
}
