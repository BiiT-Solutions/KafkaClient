package com.biit.kafka.testevents;

import java.time.LocalDateTime;

public class TestEvent {
    private String value;
    private LocalDateTime createdAt;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}
