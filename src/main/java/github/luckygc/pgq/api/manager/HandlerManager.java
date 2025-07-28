package github.luckygc.pgq.api.manager;

import github.luckygc.pgq.api.handler.MessageHandler;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface HandlerManager {

    void register(MessageHandler messageHandler);

    @Nullable
    MessageHandler get(String topic);

    List<MessageHandler> getAll();
}
