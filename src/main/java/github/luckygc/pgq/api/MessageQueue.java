package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface MessageQueue {

    void send(String message);

    void send(String message, Duration processDelay);

    void send(String message, int priority);

    void send(String message, Duration processDelay, int priority);

    void send(List<String> messages);

    void send(List<String> messages, Duration processDelay);

    void send(List<String> messages, int priority);

    void send(List<String> messages, Duration processDelay, int priority);

    @Nullable
    Message pull();

    @Nullable
    Message pull(Duration processTimeout);

    List<Message> pull(int pullCount);

    List<Message> pull(int pullCount, Duration processTimeout);
}
