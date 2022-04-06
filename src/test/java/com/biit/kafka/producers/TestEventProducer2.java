package com.biit.kafka.producers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.TestEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class TestEventProducer2 extends EventProducer<TestEvent> {

    @Autowired
    public TestEventProducer2(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

}
