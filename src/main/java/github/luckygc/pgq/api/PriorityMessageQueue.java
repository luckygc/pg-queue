package github.luckygc.pgq.api;

import java.util.List;

public interface PriorityMessageQueue extends MessagePoller {

    void send(String topic, String message, int priority);

    void send(String topic, List<String> messages, int priority);
}
