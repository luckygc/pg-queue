package github.luckygc.pgq.api;

import java.time.Duration;
import java.util.List;

public interface DelayMessageQueue extends MessagePoller{

    void send(String topic, String message, Duration processDelay);

    void send(String topic, List<String> messages, Duration processDelay);
}
