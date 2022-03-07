package com.biit.kafka;

public class TestEventDeserializer extends com.biit.kafka.EventDeserializer<TestEvent> {

    public TestEventDeserializer() {
        super(TestEvent.class);
    }
}
