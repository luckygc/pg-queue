package github.luckygc.pgq.api;

public interface QueueListener {

    String topic();

    /**
     * 不允许阻塞
     */
    void onMessageAvailable();
}
