package github.luckygc.pgq.model;

import java.time.LocalDateTime;
import org.jspecify.annotations.Nullable;
import org.springframework.jdbc.core.RowMapper;

public class Message {

    public static final RowMapper<Message> rowMapper = (rs, ignore) -> {
        Message message = new Message();
        message.setId(rs.getLong(1));
        message.setCreateTime(rs.getTimestamp(2).toLocalDateTime());
        message.setTopic(rs.getString(3));
        message.setPriority(rs.getInt(4));
        message.setPayload(rs.getString(5));
        message.setAttempt(rs.getInt(6));
        return message;
    };

    @Nullable
    private Long id;

    @Nullable
    private LocalDateTime createTime;

    @Nullable
    private String payload;

    @Nullable
    private String topic;

    @Nullable
    private Integer priority;

    @Nullable
    private Integer attempt;

    @Nullable
    public Long getId() {
        return id;
    }

    @Nullable
    public LocalDateTime getCreateTime() {
        return createTime;
    }

    @Nullable
    public String getPayload() {
        return payload;
    }

    @Nullable
    public String getTopic() {
        return topic;
    }

    @Nullable
    public Integer getPriority() {
        return priority;
    }

    @Nullable
    public Integer getAttempt() {
        return attempt;
    }

    private void setId(Long id) {
        this.id = id;
    }

    private void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    private void setPayload(String payload) {
        this.payload = payload;
    }

    private void setTopic(String topic) {
        this.topic = topic;
    }

    private void setPriority(Integer priority) {
        this.priority = priority;
    }

    private void setAttempt(Integer attempt) {
        this.attempt = attempt;
    }
}
