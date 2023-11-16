package com.biit.kafka.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.Event;
import org.springframework.stereotype.Component;

@Component
public class HistoricalEventConsumer extends HistoricalConsumer<Event> {

    public HistoricalEventConsumer(KafkaConfig kafkaConfig) {
        super(Event.class, kafkaConfig);
    }
}
