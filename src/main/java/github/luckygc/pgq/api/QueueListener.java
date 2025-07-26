package github.luckygc.pgq.api;

public interface QueueListener {

    /**
     * 不允许阻塞
     */
    void onMessageAvailable(PgQueue queue);
}
