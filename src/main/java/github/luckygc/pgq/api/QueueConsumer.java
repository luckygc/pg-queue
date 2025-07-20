package github.luckygc.pgq.api;

import github.luckygc.pgq.MessageEntity;
import java.util.List;
import java.util.Optional;

/**
 * 队列消费者接口，用于拉取和处理消息.
 */
public interface QueueConsumer {

    /**
     * 拉取单条消息.
     *
     * @param topic 队列主题
     * @return 消息实体，如果没有可用消息则返回空
     */
    Optional<MessageEntity> pullMessage(String topic);

    /**
     * 批量拉取消息.
     *
     * @param topic     队列主题
     * @param batchSize 批量大小
     * @return 消息列表
     */
    List<MessageEntity> pullMessages(String topic, int batchSize);

    /**
     * 确认消息处理完成.
     *
     * @param messageId 消息ID
     */
    void ackMessage(Long messageId);

    /**
     * 标记消息处理失败，将重新入队.
     *
     * @param messageId 消息ID
     */
    void nackMessage(Long messageId);

    /**
     * 标记消息为死信.
     *
     * @param messageId 消息ID
     */
    void deadLetterMessage(Long messageId);
}
