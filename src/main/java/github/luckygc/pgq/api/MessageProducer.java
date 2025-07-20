package github.luckygc.pgq.api;

public interface MessageProducer {

    void publish(Object message);
}
