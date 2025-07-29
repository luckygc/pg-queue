package github.luckygc.pgq;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.impl.PgmqManagerImpl;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import org.springframework.jdbc.core.JdbcTemplate;

public class Demo {

    void demo() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate();
        PgmqManager pgmqManager = new PgmqManagerImpl(jdbcTemplate);

        pgmqManager.registerHandler(new TestMessageHandler());
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
}
