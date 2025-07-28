package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface MessageQueue {

    void send(String topic, String message);

    void send(String topic, List<String> messages);

    @Nullable
    Message poll(String topic);

    List<Message> poll(String topic, int pollMax);
}
