package github.luckygc.pgq.example;

import github.luckygc.pgq.api.ProcessingMessageManager;
import github.luckygc.pgq.api.QueueManager;
import github.luckygc.pgq.api.SingleMessageHandler;
import github.luckygc.pgq.impl.QueueManagerImpl;
import github.luckygc.pgq.model.Message;
import java.sql.SQLException;
import java.time.Duration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class DemoMessageConfig {

    void demo() {

        QueueManager queueManager = new QueueManagerImpl(
                new JdbcTemplate(),
                new TransactionTemplate()
        );

        QueueManager queueManagerWithEnablePgNotify = new QueueManagerImpl(
                new JdbcTemplate(),
                new TransactionTemplate(),
                "jdbc:postgresql//127.0.0.1:5432/postgres",
                "",
                ""
        );

        queueManagerWithEnablePgNotify.queue("test").push("""
                {"name" : "xxx"}""");

        SingleMessageHandler singleMessageHandler = new SingleMessageHandler() {

            @Override
            public int threadCount() {
                return 8;
            }

            @Override
            public String topic() {
                return "test2";
            }

            @Override
            public void handle(ProcessingMessageManager messageManager, Message message) {
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
        };

        queueManagerWithEnablePgNotify.registerMessageHandler(singleMessageHandler);

        queueManagerWithEnablePgNotify.queue("test2").push("sss");

        try {
            queueManagerWithEnablePgNotify.start(10);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
