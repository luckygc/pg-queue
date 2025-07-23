package github.luckygc.pgq;

import java.util.Collections;
import java.util.List;

public class PushContext {

    private List<String> messages = Collections.emptyList();

    public void message(String message) {
        messages = List.of(message);
    }

    public void messages(List<String> messages) {
        this.messages = messages;
    }
}
