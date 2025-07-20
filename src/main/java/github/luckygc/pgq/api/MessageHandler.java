package github.luckygc.pgq.api;

public interface MessageHandler<M> {

    boolean handle(M message);
}
