package com.biit.kafka.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.logger.KafkaLogger;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

public abstract class EventConsumer<T> {

    private final KafkaConfig kafkaConfig;

    public EventConsumer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }


    private ConcurrentKafkaListenerContainerFactory<String, T> createKafkaListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(kafkaConfig.getProperties()));
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, T> eventListenerContainerFactory() {
        KafkaLogger.debug(this.getClass().getName(), "Starting the Container Factory");
        try {
            return createKafkaListenerContainerFactory();
        } catch (Exception e) {
            KafkaLogger.errorMessage(this.getClass().getName(), "Error starting the Container Factory");
            KafkaLogger.errorMessage(this.getClass().getName(), e.getMessage());
        } finally {
            KafkaLogger.debug(this.getClass().getName(), "Started Container Factory");
        }
        return null;
    }
}
