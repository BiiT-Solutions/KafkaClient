package com.biit.kafka.events.consumers;

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.events.entities.TestEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

@EnableKafka
@Configuration
public class TestTemplateEventConsumerListeners2 extends EventListener<TestEvent> {

    @KafkaListener(topics = "${spring.kafka.topic}", groupId = "2", containerFactory = "templateEventListenerContainerFactory")
    public void eventsListener(TestEvent event,
                               final @Header(KafkaHeaders.OFFSET) Integer offset,
                               final @Header(value = KafkaHeaders.KEY, required = false) String key,
                               final @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
        super.eventsListener(event, offset, key, partition, topic, timeStamp);
    }
}
