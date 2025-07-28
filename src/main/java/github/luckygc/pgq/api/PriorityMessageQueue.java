package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface PriorityMessageQueue {

    void send(String topic, String message, int priority);

    void send(String topic, List<String> messages, int priority);

    @Nullable
    Message poll(String topic);

    @Nullable
    Message poll(String topic);

    List<Message> poll(String topic, int max);
}
