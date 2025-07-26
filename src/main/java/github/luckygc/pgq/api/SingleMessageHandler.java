package github.luckygc.pgq.api;

import github.luckygc.pgq.Message;

public interface SingleMessageHandler extends MessageHandler {

    /**
     * 处理单条消息，不允许抛异常，否则打断整个处理流程
     */
    void handle(Message message);
}
