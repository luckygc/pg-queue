package github.luckygc.pgq.api.callback;

public interface MessageAvailableCallback {

    /**
     * 不允许阻塞
     */
    void onMessageAvailable(String topic);
}
