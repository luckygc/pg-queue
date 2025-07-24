package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.util.List;
import java.util.Optional;

public interface PgQueue {

    String getTopic();

    void push(String messages);

    void push(List<String> messages);

    MessageGather message(String message);

    MessageGather messages(List<String> messages);

    void listen();

    Optional<Message> pull();

    List<Message> pull(int pullCount);
}
