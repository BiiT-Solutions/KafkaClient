package com.biit.kafka.consumers;

import com.biit.kafka.KafkaConfig;
import com.biit.kafka.TestEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class TestEventConsumer extends com.biit.kafka.consumers.EventConsumer<TestEvent> {

    @Autowired
    public TestEventConsumer(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }
}
