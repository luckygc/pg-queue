package github.luckygc.pgq.api;

import github.luckygc.pgq.model.MessageDO;
import java.time.Duration;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface DelayMessageQueue {

    void send(String topic, String message, Duration processDelay);

    void send(String topic, List<String> messages, Duration processDelay);

    @Nullable
    MessageDO poll(String topic);

    @Nullable
    MessageDO poll(String topic);

    List<MessageDO> poll(String topic, int max);
}
