package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.time.Duration;
import java.util.List;

public interface MessageManager {

    void complete(Message message, boolean delete);

    void complete(List<Message> messages, boolean delete);

    void retry(Message message, Duration processDelay);

    void retry(List<Message> messages, Duration processDelay);

    void dead(Message message);

    void dead(List<Message> messages);
}
