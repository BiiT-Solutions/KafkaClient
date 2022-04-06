package com.biit.kafka.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.TestEvent;
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
