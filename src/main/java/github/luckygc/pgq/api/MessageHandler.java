package github.luckygc.pgq.api;

public interface MessageHandler<M> {

    Class<M> getMessageCls();

    boolean handle(M message);
}
