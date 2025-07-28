package github.luckygc.pgq.api.callback;

public interface MessageAvailabilityCallback {

    String topic();

    /**
     * 不允许阻塞
     */
    void onMessageAvailable();
}
