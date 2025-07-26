package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.time.Duration;
import java.util.List;

public interface MessageManager {

    void delete(Message message);

    void delete(List<Message> messages);

    void complete(Message message);

    void complete(List<Message> messages);

    void retry(Message message, Duration processDelay);

    void retry(List<Message> messages, Duration processDelay);

    void dead(Message message);

    void dead(List<Message> messages);
}
