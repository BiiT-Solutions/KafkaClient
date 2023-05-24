package com.biit.kafka.events.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.consumers.EventConsumer;
import com.biit.kafka.events.entities.TestEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class TestEventConsumer extends EventConsumer<TestEvent> {

    @Autowired
    public TestEventConsumer(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }
}
