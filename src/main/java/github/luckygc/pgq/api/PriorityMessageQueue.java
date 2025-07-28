package github.luckygc.pgq.api;

import github.luckygc.pgq.model.MessageDO;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface PriorityMessageQueue {

    void send(String topic, String message, int priority);

    void send(String topic, List<String> messages, int priority);

    @Nullable
    MessageDO poll(String topic);

    @Nullable
    MessageDO poll(String topic);

    List<MessageDO> poll(String topic, int max);
}
