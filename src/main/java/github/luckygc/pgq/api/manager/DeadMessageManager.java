package github.luckygc.pgq.api.manager;

public interface DeadMessageManager {

    void retry(String topic);

    void delete(String topic);
}
