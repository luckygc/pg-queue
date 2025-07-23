package github.luckygc.pgq.api;

import java.time.Duration;

public interface MessageGather {

    MessageGather priority(int priority);

    MessageGather processDelay(Duration delay);

    void push();
}
