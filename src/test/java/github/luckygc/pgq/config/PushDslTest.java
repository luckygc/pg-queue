package github.luckygc.pgq.config;

import github.luckygc.pgq.api.PgQueue;
import github.luckygc.pgq.api.PgqManager;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

public class PushDslTest {

    PgqManager pgqManager;

    @Test
    void test() {
        PgQueue queue = pgqManager.registerQueue("topic");

        queue.push("m1");
        queue.push(List.of("m2", "m3"));

        queue.message("m4").priority(999).processDelay(Duration.ofMinutes(10)).push();
        queue.message("m5").processDelay(Duration.ofHours(1)).push();


    }
}
