package com.biit.kafka.events.entities;

import com.biit.kafka.events.Event;

public class TestEvent extends Event<TestPayload> {

    public TestEvent(TestPayload testPayload) {
        super(testPayload);
    }

    public TestEvent() {
        super();
    }
}
