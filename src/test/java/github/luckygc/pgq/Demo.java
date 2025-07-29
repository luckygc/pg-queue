package github.luckygc.pgq;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.model.Message;
import java.time.Duration;

public class Demo {

    private PgmqManager pgmqManager;

    void demo() {
        pgmqManager.registerHandler(new TestMessageHandler());
        pgmqManager.registerHandler(new Test2MessageHandler());

        pgmqManager.queue().send("test", "hello");
        pgmqManager.queue().send("test2", "hello");
        pgmqManager.delayQueue().send("test", "hello2", Duration.ofMinutes(15));
        pgmqManager.priorityQueue().send("test2", "hello3", 2);

        pgmqManager.queue().send("test3", "xxx");
        Message message = pgmqManager.queue().poll("test3");
        if (message != null) {
            message.delete();
        }
    }

    static class TestMessageHandler implements MessageHandler {

        @Override
        public String topic() {
            return "test";
        }

        @Override
        public void handle(Message message) {
            try {
                String payload = message.getPayload();
                // handle
                message.delete();
            } catch (IllegalStateException e) {
                if (message.getAttempt() >= 3) {
                    message.dead();
                } else {
                    message.retry(Duration.ofMinutes(10));
                }
            } catch (Exception e) {
                message.dead();
            }
        }
    }

    static class Test2MessageHandler implements MessageHandler {

        @Override
        public String topic() {
            return "test2";
        }

        @Override
        public int threadCount() {
            int cpuCores = Runtime.getRuntime().availableProcessors();
            return cpuCores * 2 + 1;
        }

        @Override
        public void handle(Message message) {
            try {
                String payload = message.getPayload();
                // handle
                message.delete();
            } catch (IllegalStateException e) {
                if (message.getAttempt() >= 3) {
                    message.dead();
                } else {
                    message.retry(Duration.ofDays(1));
                }
            } catch (Exception e) {
                message.dead();
            }
        }
    }
}
