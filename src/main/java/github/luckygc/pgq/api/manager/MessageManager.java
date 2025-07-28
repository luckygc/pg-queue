package github.luckygc.pgq.api.manager;

import java.time.Duration;

/**
 * 管理拉取到的消息
 */
public interface MessageManager {

    void dead(Long id);

    void delete(Long id);

    /**
     * 立即重试
     */
    void retry(Long id);

    /**
     * 延迟重试
     */
    void retry(Long id, Duration processDelay);
}
