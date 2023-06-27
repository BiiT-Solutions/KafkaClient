package com.biit.kafka.events.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.consumers.HistoricalEventConsumer;
import com.biit.kafka.events.Event;
import org.springframework.stereotype.Component;

@Component
public class TestHistoricalEventConsumer extends HistoricalEventConsumer<Event> {

    public TestHistoricalEventConsumer(KafkaConfig kafkaConfig) {
        super(Event.class, kafkaConfig);
    }
}
