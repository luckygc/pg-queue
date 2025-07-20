package github.luckygc.pgq.api;

public interface MessagePusher<M> {

    void push(M message);
}
