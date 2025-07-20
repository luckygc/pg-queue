package github.luckygc.pgq.api;

import github.luckygc.pgq.model.MessageEntity;
import java.util.List;
import java.util.Optional;

/**
 * 拉取消息
 */
public interface MessagePuller {

    Optional<MessageEntity> pull();

    List<MessageEntity> pull(long pullCount);
}
