package github.luckygc.pgq;

import java.time.LocalDateTime;

public class Message {

    private Long id;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

    private String payload;

    private String topic;

    private Integer priority;

    private LocalDateTime visibleTime;

    private Integer attempt;

    public Long getId() {
        return id;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public LocalDateTime getUpdateTime() {
        return updateTime;
    }

    public String getPayload() {
        return payload;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getPriority() {
        return priority;
    }

    public LocalDateTime getVisibleTime() {
        return visibleTime;
    }

    public Integer getAttempt() {
        return attempt;
    }

    protected void setId(Long id) {
        this.id = id;
    }

    protected void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    protected void setUpdateTime(LocalDateTime updateTime) {
        this.updateTime = updateTime;
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

    protected void setVisibleTime(LocalDateTime visibleTime) {
        this.visibleTime = visibleTime;
    }

    protected void setAttempt(Integer attempt) {
        this.attempt = attempt;
    }
}
