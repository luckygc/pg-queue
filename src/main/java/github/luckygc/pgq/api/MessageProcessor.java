package github.luckygc.pgq.api;

public interface MessageProcessor {

    String getTopic();

    /**
     * 不要阻塞调用
     *
     * @param pgQueue topic对应队列
     */
    void onMessageWaitProcess(PgQueue pgQueue);
}
