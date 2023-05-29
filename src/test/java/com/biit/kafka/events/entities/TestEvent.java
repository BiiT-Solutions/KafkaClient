package com.biit.kafka.events.entities;

import com.biit.kafka.events.Event;
import com.fasterxml.jackson.core.type.TypeReference;

public class TestEvent extends Event<TestPayload> {

    public TestEvent() {
        super();
    }

    public TestEvent(TestPayload testPayload) {
        super(testPayload);
    }

    protected TypeReference<TestPayload> getJsonParser() {
        return new TypeReference<>() {
        };
    }
}
