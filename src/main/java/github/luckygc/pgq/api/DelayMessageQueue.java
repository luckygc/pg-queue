package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface DelayMessageQueue {

    void send(String topic, String message, Duration processDelay);

    void send(String topic, List<String> messages, Duration processDelay);

    @Nullable
    Message poll(String topic);

    @Nullable
    Message poll(String topic);

    List<Message> poll(String topic, int max);
}
