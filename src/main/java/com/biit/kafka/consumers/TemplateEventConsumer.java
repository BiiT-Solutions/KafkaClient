package com.biit.kafka.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.Event;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class TemplateEventConsumer<V extends Event<?>> {
    private final KafkaConfig kafkaConfig;
    private final Class<V> type;


    public TemplateEventConsumer(Class<V> type, KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.type = type;
    }

    @Bean
    public ConsumerFactory<String, V> typeConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaConfig.getProperties(),
                new StringDeserializer(),
                new JsonDeserializer<>(type));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, V> templateEventListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(typeConsumerFactory());
        return factory;
    }
}
