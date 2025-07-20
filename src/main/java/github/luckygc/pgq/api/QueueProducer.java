package github.luckygc.pgq.api;

public interface QueueProducer {

    void publish(Object message);
}
