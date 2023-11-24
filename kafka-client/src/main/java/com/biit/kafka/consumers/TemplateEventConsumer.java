package com.biit.kafka.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.EventDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class TemplateEventConsumer {
    private final KafkaConfig kafkaConfig;


    public TemplateEventConsumer(@Autowired(required = false) KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public ConsumerFactory<String, Event> typeConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaConfig.getConsumerProperties(),
                new StringDeserializer(),
                new EventDeserializer());
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Event> templateEventListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, Event> factory = new ConcurrentKafkaListenerContainerFactory<>();
        if (kafkaConfig != null) {
            factory.setConsumerFactory(typeConsumerFactory());
        }
        return factory;
    }
}
