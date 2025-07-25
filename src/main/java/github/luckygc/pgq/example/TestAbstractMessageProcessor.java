package github.luckygc.pgq.example;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.PgqManager;
import github.luckygc.pgq.support.AbstractMessageProcessor;
import java.util.List;


public class TestAbstractMessageProcessor extends AbstractMessageProcessor {

    private static final String TOPIC = "test";

    public TestAbstractMessageProcessor(PgqManager pgqManager) {
        super(TOPIC, pgqManager.messageManager(), 4);
    }

    @Override
    protected int messageHandleCount() {
        return 0;
    }

    @Override
    protected void handleMessage(Message message) {

    }

    @Override
    protected void handleMessages(List<Message> messages) {

    }
}
