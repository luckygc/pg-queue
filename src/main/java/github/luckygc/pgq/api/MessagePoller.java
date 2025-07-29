package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface MessagePoller {

    /**
     * 非阻塞
     */
    @Nullable
    Message poll(String topic);

    /**
     * 非阻塞
     */
    List<Message> poll(String topic, int pollMax);
}
