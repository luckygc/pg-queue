package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.util.List;

public interface MultiMessageHandler extends MessageHandler {

    void handle(List<Message> messages);
}
