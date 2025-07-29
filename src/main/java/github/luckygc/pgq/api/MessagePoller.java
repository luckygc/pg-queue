package github.luckygc.pgq.api;

import github.luckygc.pgq.model.Message;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface MessagePoller {

    @Nullable
    Message poll(String topic);

    List<Message> poll(String topic, int pollMax);
}
