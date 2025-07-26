package github.luckygc.pgq.api;

public interface DeadMessageManger {

    void retryDeadMessages(String topic);
}
