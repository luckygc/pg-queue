package github.luckygc.pgq.impl;

import github.luckygc.pgq.Message;
import github.luckygc.pgq.api.MessageGather;
import github.luckygc.pgq.api.PgQueue;
import java.time.Duration;
import java.util.List;
import org.jspecify.annotations.Nullable;

public class PgqQueueImpl implements PgQueue {

    @Override
    public String getTopic() {
        return "";
    }

    @Override
    public void push(@Nullable String message) {

    }

    @Override
    public void push(@Nullable List<String> messages) {

    }

    @Override
    public MessageGather message(@Nullable String message) {
        return null;
    }

    @Override
    public MessageGather messages(@Nullable List<String> messages) {
        return null;
    }

    @Override
    public @Nullable Message pull() {
        return null;
    }

    @Override
    public List<Message> pull(int pullCount) {
        return List.of();
    }

    @Override
    public void delete(Message message) {

    }

    @Override
    public void delete(List<Message> messages) {

    }

    @Override
    public void complete(Message message) {

    }

    @Override
    public void complete(List<Message> messages) {

    }

    @Override
    public void retry(Message message, Duration processDelay) {

    }

    @Override
    public void retry(List<Message> messages, Duration processDelay) {

    }

    @Override
    public void dead(Message message) {

    }

    @Override
    public void dead(List<Message> messages) {

    }
}
