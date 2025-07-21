package github.luckygc.pgq;

import static org.assertj.core.api.Assertions.assertThat;

import github.luckygc.pgq.api.MessageHandler;
import github.luckygc.pgq.api.MessageSerializable;
import github.luckygc.pgq.api.PgQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

public class PgQueueTest {

    static JdbcClient jdbcClient;
    static JdbcTemplate jdbcTemplate;
    static PgQueue<String> pgQueue;

    private static final List<String> messages = new ArrayList<>();

    @BeforeAll
    static void beforeAll() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL("jdbc:postgresql://localhost:5432/repodar");
        dataSource.setUser("postgres");
        jdbcClient = JdbcClient.create(dataSource);
        jdbcTemplate = new JdbcTemplate(dataSource);

        MessageSerializable<String> messageSerializable = new MessageSerializable<>() {
            @Override
            public String serialize(String message) {
                return message;
            }

            @Override
            public String deserialize(String payload) {
                return payload;
            }
        };

        MessageHandler<String> messageHandler = message -> {
            if (message.equals("3")) {
                return false;
            }

            messages.add(message);
            return true;
        };

        pgQueue = PgQueueBuilder.<String>create()
                .jdbcTemplate(jdbcTemplate)
                .topic("__test__")
                .messageSerializer(messageSerializable)
                .messageHandler(messageHandler)
                .build();
    }

    @Test
    void test() {
        pgQueue.push("1");
        pgQueue.push("2", 2);
        pgQueue.push("3");

        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));

        assertThat(messages).hasSize(2).containsExactlyInAnyOrder("2", "1");

        {
            long deleteCompleted = pgQueue.deleteCompleted();
            assertThat(deleteCompleted).isEqualTo(2L);

            long deletedDead = pgQueue.deleteDead();
            assertThat(deletedDead).isEqualTo(1L);
        }

        pgQueue.push(List.of("5", "6"));

        {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            long deleteCompleted = pgQueue.deleteCompleted();
            assertThat(deleteCompleted).isEqualTo(2L);
        }
    }
}
