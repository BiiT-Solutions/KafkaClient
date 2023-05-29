package com.biit.kafka.events.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.consumers.HistoricalEventConsumer;
import com.biit.kafka.events.entities.TestEvent;
import org.springframework.stereotype.Component;

@Component
public class TestHistoricalEventConsumer extends HistoricalEventConsumer<TestEvent> {

    public TestHistoricalEventConsumer(KafkaConfig kafkaConfig) {
        super(TestEvent.class, kafkaConfig);
    }
}
