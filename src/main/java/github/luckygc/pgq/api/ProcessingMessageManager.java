package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.util.List;

/**
 * 管理拉取到的消息
 */
public interface ProcessingMessageManager {

    void complete(Message message);

    void complete(List<Message> messages);

    void dead(Message message);

    void dead(List<Message> messages);

    void delete(Message message);

    void delete(List<Message> messages);

    /**
     * 立即重试
     */
    void retry(Message message);

    /**
     * 延迟重试
     */
    void retry(Message message, Duration processDelay);

    /**
     * 立即重试
     */
    void retry(List<Message> messages);

    /**
     * 延迟重试
     */
    void retry(List<Message> messages, Duration processDelay);
}
