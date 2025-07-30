package github.luckygc.pgq;

import static org.assertj.core.api.Assertions.assertThat;

import github.luckygc.pgq.api.PgmqManager;
import github.luckygc.pgq.api.handler.MessageHandler;
import github.luckygc.pgq.impl.PgmqManagerImpl;
import github.luckygc.pgq.integration.BaseIntegrationTest;
import github.luckygc.pgq.model.Message;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class ClusterTest extends BaseIntegrationTest {

    private PgmqManager pgmqManager;
    private PgmqManager pgmqManager2;

    @BeforeEach
    void setUp() {
        pgmqManager = new PgmqManagerImpl(jdbcTemplate, postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword());
        pgmqManager2 = new PgmqManagerImpl(jdbcTemplate, postgres.getJdbcUrl(), postgres.getUsername(),
                postgres.getPassword());
    }

    @AfterEach
    void tearDown() {
        if (pgmqManager != null) {
            pgmqManager.shutdown();
        }

        if (pgmqManager2 != null) {
            pgmqManager2.shutdown();
        }
    }

    @Test
    @DisplayName("一个应用发送消息，另一个应用应该收到消息可用通知")
    void test() {
        pgmqManager.queue().send("test", "xxx");
        AtomicReference<Message> polledMessage = new AtomicReference<>();
        AtomicReference<LocalDateTime> polledTime = new AtomicReference<>();

        pgmqManager2.registerHandler(new MessageHandler() {
            @Override
            public String topic() {
                return "test";
            }

            @Override
            public void handle(Message message) {
                polledMessage.set(message);
                polledTime.set(LocalDateTime.now());
            }
        });

        LockSupport.parkNanos(Duration.ofSeconds(2).toNanos());

        assertThat(polledMessage.get()).isNotNull();
        assertThat(polledMessage.get().getTopic()).isEqualTo("test");
        assertThat(polledMessage.get().getPayload()).isEqualTo("xxx");
        Duration duration = Duration.between(polledMessage.get().getCreateTime(), polledTime.get());
        assertThat(duration).isLessThan(Duration.ofSeconds(1));
    }
}
