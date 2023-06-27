package com.biit.kafka.events.entities;

import com.biit.kafka.events.Event;

public class TestEvent extends Event {

    public TestEvent() {
        super();
    }

    public TestEvent(TestPayload testPayload) {
        super(testPayload);
    }
}
