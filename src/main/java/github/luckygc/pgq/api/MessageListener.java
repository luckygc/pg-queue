package github.luckygc.pgq.api;

public interface MessageListener {

    /**
     * 不允许阻塞
     */
    void onMessageAvailable();
}
