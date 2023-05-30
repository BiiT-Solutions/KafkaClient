package com.biit.kafka.events.consumers;

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.events.entities.TestEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@EnableKafka
@Configuration
public class TestTemplateEventConsumerListeners2 extends EventListener<TestEvent> {

    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "2", containerFactory = "templateEventListenerContainerFactory")
    public void eventsListener(TestEvent event) {
        super.eventsListener(event);
    }
}
