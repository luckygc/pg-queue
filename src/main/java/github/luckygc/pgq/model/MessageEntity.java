package github.luckygc.pgq.model;

import java.time.LocalDateTime;

public class MessageEntity {

    private Long id;

    private LocalDateTime createTime;

    private String payload;

    private String topic;

    private MessageStatus status;

    private Integer priority;

    private LocalDateTime nextProcessTime;

    private Integer attempt;

    private Integer maxAttempt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public MessageStatus getStatus() {
        return status;
    }

    public void setStatus(MessageStatus status) {
        this.status = status;
    }

    public LocalDateTime getNextProcessTime() {
        return nextProcessTime;
    }

    public void setNextProcessTime(LocalDateTime nextProcessTime) {
        this.nextProcessTime = nextProcessTime;
    }

    public Integer getAttempt() {
        return attempt;
    }

    public void setAttempt(Integer attempt) {
        this.attempt = attempt;
    }

    public Integer getMaxAttempt() {
        return maxAttempt;
    }

    public void setMaxAttempt(Integer maxAttempt) {
        this.maxAttempt = maxAttempt;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }
}
