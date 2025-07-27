package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface DatabaseQueue {

    void push(String message);

    void push(String message, Duration processDelay);

    void push(String message, int priority);

    void push(String message, Duration processDelay, int priority);

    void push(List<String> messages);

    void push(List<String> messages, Duration processDelay);

    void push(List<String> messages, int priority);

    void push(List<String> messages, Duration processDelay, int priority);

    @Nullable
    Message pull();

    @Nullable
    Message pull(Duration processTimeout);

    List<Message> pull(int pullCount);

    List<Message> pull(int pullCount, Duration processTimeout);
}
