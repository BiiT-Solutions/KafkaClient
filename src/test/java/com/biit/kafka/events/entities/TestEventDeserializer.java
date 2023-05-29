package com.biit.kafka.events.entities;

import com.biit.kafka.EventDeserializer;

public class TestEventDeserializer extends EventDeserializer<TestPayload> {

    public TestEventDeserializer() {
        super(TestPayload.class);
    }
}
