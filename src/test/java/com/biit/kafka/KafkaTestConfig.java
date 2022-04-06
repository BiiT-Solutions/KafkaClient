package com.biit.kafka;

import com.biit.kafka.config.KafkaConfig;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class KafkaTestConfig extends KafkaConfig {

    @Override
    public Map<String, Object> getProperties() {
        final Map<String, Object> props = super.getProperties();
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, TestEventDeserializer.class);
        return props;
    }
}
