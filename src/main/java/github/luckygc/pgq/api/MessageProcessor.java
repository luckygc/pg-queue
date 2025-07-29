package github.luckygc.pgq.api;

public interface MessageProcessor {

    String topic();

    void process();
}
