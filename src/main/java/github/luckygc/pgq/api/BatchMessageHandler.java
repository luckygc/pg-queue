package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.util.List;

@FunctionalInterface
public interface BatchMessageHandler {

    void handle(List<Message> messages);
}
