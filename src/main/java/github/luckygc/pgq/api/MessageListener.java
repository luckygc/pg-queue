package github.luckygc.pgq.api;

public interface MessageListener {

    String topic();

    /**
     * 不允许阻塞
     */
    void onMessageAvailable();
}
