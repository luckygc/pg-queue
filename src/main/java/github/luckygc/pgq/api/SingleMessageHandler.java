package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;

public interface SingleMessageHandler extends MessageHandler {

    void handle(Message message);
}
