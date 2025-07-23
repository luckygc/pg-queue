package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.util.List;
import java.util.Optional;

public interface PgQueue {

    void push(String messages);

    void push(List<String> messages);

    MessageGather message(String message);

    MessageGather messages(List<String> messages);

    Optional<Message> pull();

    List<Message> pull(int pullCount);
}
