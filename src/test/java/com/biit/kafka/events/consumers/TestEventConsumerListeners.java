package com.biit.kafka.events.consumers;

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.events.entities.TestPayload;
import org.springframework.kafka.annotation.KafkaListener;

//@EnableKafka
//@Configuration
public class TestEventConsumerListeners extends EventListener<TestPayload> {

    @KafkaListener(topics = "${kafka.topic}", groupId = "${kafka.group.id}", clientIdPrefix = "#{T(java.util.UUID).randomUUID().toString()}", containerFactory = "eventListenerContainerFactory")
    public void eventsListener(TestPayload event) {
        super.eventsListener(event);
    }
}
