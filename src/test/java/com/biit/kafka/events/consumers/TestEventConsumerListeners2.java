package com.biit.kafka.events.consumers;

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.events.entities.TestEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@EnableKafka
@Configuration
public class TestEventConsumerListeners2 extends EventListener<TestEvent> {

    @KafkaListener(topics = "${kafka.topic}", groupId = "2", clientIdPrefix = "#{T(java.util.UUID).randomUUID().toString()}", containerFactory = "eventListenerContainerFactory")
    public void eventsListener(TestEvent event) {
        super.eventsListener(event);
    }

}
