package com.biit.kafka.producers;

import com.biit.kafka.KafkaConfig;
import com.biit.kafka.TestEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class TestEventProducer extends com.biit.kafka.producers.EventProducer<TestEvent> {

    @Autowired
    public TestEventProducer(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

}
