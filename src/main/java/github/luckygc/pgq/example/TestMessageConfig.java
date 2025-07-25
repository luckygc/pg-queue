package github.luckygc.pgq.example;

import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.PgqManager;

public class TestMessageConfig {

    public PgqManager pgqManager() {
        return null;
    }

    public PgQueue testQueue(PgqManager pgqManager) {
        PgQueue pgQueue = pgqManager.registerQueue("test");
        TestAbstractMessageProcessor testMessageProcessor = new TestAbstractMessageProcessor();
        pgqManager.registerMessageProcessor(testMessageProcessor);
        return null;
    }
}
