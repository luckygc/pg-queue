package github.luckygc.pgq.api.manager;

import github.luckygc.pgq.api.handler.BatchMessageHandler;
import github.luckygc.pgq.api.handler.SingleMessageHandler;

public interface HandlerManager {

    void registerMessageHandler(SingleMessageHandler messageHandler);

    void registerMessageHandler(BatchMessageHandler messageHandler);
}
