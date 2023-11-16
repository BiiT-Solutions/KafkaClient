package com.biit.kafka.producers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.KafkaEventTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

public class TemplateEventProducer {
    private final KafkaConfig kafkaConfig;

    public TemplateEventProducer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Bean
    public ProducerFactory<String, Event> producerFactory() {
        return new DefaultKafkaProducerFactory<>(kafkaConfig.getProperties());
    }

    @Bean
    public KafkaTemplate<String, Event> kafkaTemplate() {
        return new KafkaEventTemplate(kafkaConfig, producerFactory());
    }
}
