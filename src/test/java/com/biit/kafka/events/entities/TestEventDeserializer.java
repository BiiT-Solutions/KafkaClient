package com.biit.kafka.events.entities;

import com.biit.kafka.EventDeserializer;

public class TestEventDeserializer extends EventDeserializer<TestEvent> {

    public TestEventDeserializer() {
        super(TestEvent.class);
    }
}
