package github.luckygc.pgq;

import github.luckygc.pgq.model.Message;
import github.luckygc.pgq.model.MessageDO;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class Utils {

    private Utils() {
    }

    public static void checkMessagesNotEmpty(@Nullable List<?> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages不能为空");
        }
    }

    public static void checkDurationIsPositive(Duration duration) {
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("duration必须大于0秒");
        }
    }

    public static void checkMaxPollRange(int maxPoll) {
        if (maxPoll < 1 || maxPoll > 5000) {
            throw new IllegalArgumentException("maxPoll必须在1-5000之间");
        }
    }
}
