package com.biit.kafka.events.entities;

import com.biit.kafka.events.EventPayload;

public class TestEventPayload implements EventPayload {
    private String value;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
