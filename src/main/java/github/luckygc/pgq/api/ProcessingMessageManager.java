package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.time.Duration;
import java.util.List;

/**
 * 管理正在处理中的消息
 */
public interface ProcessingMessageManager {

    void complete(Message message);

    void complete(List<Message> messages);

    void dead(Message message);

    void dead(List<Message> messages);

    void delete(Message message);

    void delete(List<Message> messages);

    void retry(Message message);

    void retry(Message message, Duration processDelay);

    void retry(List<Message> messages);

    void retry(List<Message> messages, Duration processDelay);
}
