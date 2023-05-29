package com.biit.kafka.events.producers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.entities.TestPayload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class TestEventProducer extends com.biit.kafka.producers.EventProducer<TestPayload> {

    @Autowired
    public TestEventProducer(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

}
