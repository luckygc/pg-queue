package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;
import java.util.List;
import org.jspecify.annotations.Nullable;

public interface PgQueue extends MessageManager {

    String getTopic();

    void push(@Nullable String message);

    void push(@Nullable List<String> messages);

    MessageGather message(@Nullable String message);

    MessageGather messages(@Nullable List<String> messages);

    @Nullable
    Message pull();

    /**
     * @param pullCount 拉取数量，范围[1,5000]
     */
    List<Message> pull(int pullCount);
}
