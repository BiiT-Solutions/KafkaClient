package com.biit.kafka.testevents;

import com.biit.kafka.EventDeserializer;

public class TestEventDeserializer extends com.biit.kafka.EventDeserializer<TestEvent> {

    public TestEventDeserializer() {
        super(TestEvent.class);
    }
}
