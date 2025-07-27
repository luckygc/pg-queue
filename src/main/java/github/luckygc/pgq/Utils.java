package github.luckygc.pgq;

import java.util.List;
import org.jspecify.annotations.Nullable;

public class Utils {

    private Utils() {
    }

    public static void checkMessagesNotEmpty(@Nullable List<?> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages不能为空");
        }
    }
}
