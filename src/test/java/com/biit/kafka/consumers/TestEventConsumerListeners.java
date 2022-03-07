package com.biit.kafka.consumers;

import com.biit.kafka.TestEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@EnableKafka
@Configuration
public class TestEventConsumerListeners extends EventListener<TestEvent> {

    @KafkaListener(topics = "${kafka.topic}", clientIdPrefix = "#{T(java.util.UUID).randomUUID().toString()}", containerFactory = "eventListenerContainerFactory")
    public void eventsListener(TestEvent event) {
        super.eventsListener(event);
    }
}
