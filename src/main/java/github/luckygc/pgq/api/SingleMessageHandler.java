package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;

@FunctionalInterface
public interface SingleMessageHandler {

    void handle(Message message);
}
