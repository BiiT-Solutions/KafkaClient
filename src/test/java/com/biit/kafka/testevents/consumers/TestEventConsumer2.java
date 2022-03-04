package com.biit.kafka.testevents.consumers;

import com.biit.kafka.KafkaConfig;
import com.biit.kafka.consumers.EventConsumer;
import com.biit.kafka.testevents.TestEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class TestEventConsumer2 extends EventConsumer<TestEvent> {

    @Autowired
    public TestEventConsumer2(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }
}
