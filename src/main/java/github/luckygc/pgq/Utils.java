package github.luckygc.pgq;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class Utils {

    private Utils() {
    }

    public static Message buildMessageObj(String topic, String message, int priority) {
        Message messageObj = new Message();
        messageObj.setCreateTime(LocalDateTime.now());
        messageObj.setTopic(topic);
        messageObj.setPriority(priority);
        messageObj.setPayload(message);
        messageObj.setAttempt(0);
        return messageObj;
    }

    public static List<Message> buildMessageObjs(String topic, List<String> messages, int priority) {
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

    public static void checkNotEmpty(List<Message> messages) {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("messages不能为空");
        }
    }
}
