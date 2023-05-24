package com.biit.kafka.events.entities;

import java.time.LocalDateTime;
import java.util.Objects;

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

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TestEvent testEvent = (TestEvent) o;
        return Objects.equals(getValue(), testEvent.getValue()) && Objects.equals(getCreatedAt(), testEvent.getCreatedAt());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue(), getCreatedAt());
    }
}
