package github.luckygc.pgq.api;

import java.time.Duration;
import org.jspecify.annotations.Nullable;

public interface MessageGather {

    MessageGather priority(int priority);

    MessageGather processDelay(@Nullable Duration delay);

    void push();
}
