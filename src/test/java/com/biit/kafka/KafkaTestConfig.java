package com.biit.kafka;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.entities.TestEventDeserializer;
import org.springframework.stereotype.Service;

@Service
public class KafkaTestConfig extends KafkaConfig {


    @Override
    protected Class<?> getEventDeserializerClass() {
        return TestEventDeserializer.class;
    }
}
