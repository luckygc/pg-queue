package github.luckygc.pgq;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public class Utils {

    private Utils() {
    }

    public static Message buildMessageObj(String topic, String message, int priority) {
        Objects.requireNonNull(topic);
        Objects.requireNonNull(message);

        Message messageObj = new Message();
        messageObj.setCreateTime(LocalDateTime.now());
        messageObj.setTopic(topic);
        messageObj.setPriority(priority);
        messageObj.setPayload(message);
        messageObj.setAttempt(0);
        return messageObj;
    }

    public static List<Message> buildMessageObjs(String topic, List<String> messages, int priority) {
        Objects.requireNonNull(topic);
        Utils.checkMessagesNotEmpty(messages);

        List<Message> messagesObjs = new ArrayList<>(messages.size());
        LocalDateTime now = LocalDateTime.now();
        for (String message : messages) {
            Message messageObj = new Message();
            messageObj.setCreateTime(now);
            messageObj.setTopic(topic);
            messageObj.setPriority(priority);
            messageObj.setPayload(message);
            messageObj.setAttempt(0);
            messagesObjs.add(messageObj);
        }

        return messagesObjs;
    }

    public static void checkMessagesNotEmpty(@Nullable List<?> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages不能为空");
        }
    }
}
