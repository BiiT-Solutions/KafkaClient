package com.biit.kafka.producers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.KafkaEventTemplate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

public class TemplateEventProducer<E extends Event<?>> {
    private final KafkaConfig kafkaConfig;

    public TemplateEventProducer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Bean
    public ProducerFactory<String, E> producerFactory() {
        final Map<String, Object> configProps = kafkaConfig.getProperties();
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, E> kafkaTemplate() {
        return new KafkaEventTemplate<>(kafkaConfig, producerFactory());
    }
}
