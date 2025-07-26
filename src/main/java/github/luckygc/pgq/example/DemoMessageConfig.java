package github.luckygc.pgq.example;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.MessageManager;
import github.luckygc.pgq.api.PgqManager;
import github.luckygc.pgq.api.QueueManager;
import github.luckygc.pgq.api.SingleMessageHandler;
import github.luckygc.pgq.impl.PgqManagerImpl;
import java.time.Duration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class DemoMessageConfig {

    void demo() {

        PgqManager pgqManager = new PgqManagerImpl(
                "jdbc:postgresql//127.0.0.1:5432/postgres",
                "",
                "",
                new JdbcTemplate(),
                new TransactionTemplate()
        );

        QueueManager testQueueManager = pgqManager.register("test");
        testQueueManager.queue().push("""
                {"name" : "xxx"}""");
        testQueueManager.queue();

        QueueManager testQueue2 = pgqManager.register("test2", new SingleMessageHandler() {

            @Override
            public int threadCount() {
                return 8;
            }

            @Override
            public void handle(MessageManager messageManager, Message message) {
                if (message.getPayload() == null) {
                    messageManager.delete(message);
                    return;
                }

                try {
                    // handle
                } catch (IllegalStateException e) {
                    Integer attempt = message.getAttempt();
                    if (attempt != null && attempt >= 3) {
                        messageManager.dead(message);
                    } else {
                        messageManager.retry(message, Duration.ofMinutes(10));
                    }
                } catch (Throwable t) {
                    messageManager.dead(message);
                }

                messageManager.delete(message);
                // messageManager.complete(message);
            }
        });
    }
}
