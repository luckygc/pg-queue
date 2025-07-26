package github.luckygc.pgq;

import java.time.LocalDateTime;
import org.jspecify.annotations.Nullable;

public class Message {

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

    protected void setId(Long id) {
        this.id = id;
    }

    protected void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    protected void setPayload(String payload) {
        this.payload = payload;
    }

    protected void setTopic(String topic) {
        this.topic = topic;
    }

    protected void setPriority(Integer priority) {
        this.priority = priority;
    }

    protected void setAttempt(Integer attempt) {
        this.attempt = attempt;
    }
}
