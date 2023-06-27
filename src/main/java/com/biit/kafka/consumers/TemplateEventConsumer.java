package com.biit.kafka.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.EventDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

@Component
public class TemplateEventConsumer {
    private final KafkaConfig kafkaConfig;


    public TemplateEventConsumer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Bean
    public ConsumerFactory<String, Event> typeConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaConfig.getProperties(),
                new StringDeserializer(),
                new EventDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Event> templateEventListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, Event> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(typeConsumerFactory());
        return factory;
    }
}
