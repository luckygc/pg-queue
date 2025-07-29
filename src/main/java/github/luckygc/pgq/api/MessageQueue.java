package github.luckygc.pgq.api;

import java.util.List;

public interface MessageQueue extends MessagePoller {

    void send(String topic, String message);

    void send(String topic, List<String> messages);
}
