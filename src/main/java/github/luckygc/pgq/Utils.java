package github.luckygc.pgq;

import github.luckygc.pgq.model.Message;
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

    public static Long[] getIdArray(List<Message> messages) {
        Long[] ids = new Long[messages.size()];
        int i = 0;
        for (Message message : messages) {
            ids[i++] = Objects.requireNonNull(message.getId());
        }

        return ids;
    }
}
