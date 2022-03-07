package com.biit.kafka.consumers;

import com.biit.kafka.KafkaConfig;
import com.biit.kafka.logger.KafkaLogger;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

public abstract class EventConsumer<T> {

    private final KafkaConfig kafkaConfig;

    public EventConsumer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }


    private ConcurrentKafkaListenerContainerFactory<String, T> kafkaListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(kafkaConfig.getProperties()));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, T> eventListenerContainerFactory() {
        KafkaLogger.debug(this.getClass().getName(), "Starting FactConsumer");
        try {
            return kafkaListenerContainerFactory();
        } catch (Exception e) {
            KafkaLogger.errorMessage(this.getClass().getName(), "Error starting the FactConsumer");
            KafkaLogger.errorMessage(this.getClass().getName(), e.getMessage());
        } finally {
            KafkaLogger.debug(this.getClass().getName(), "Started FactConsumer");
        }
        return null;
    }
}
