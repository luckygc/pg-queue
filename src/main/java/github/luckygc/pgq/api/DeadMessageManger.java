package github.luckygc.pgq.api;

public interface DeadMessageManger {

    void retry(String topic);

    void delete(String topic);
}
