package com.biit.kafka.events;

import com.biit.kafka.config.KafkaConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaEventTemplate<K, V extends Event<?>> extends KafkaTemplate<K, V> {
    private final KafkaConfig kafkaConfig;

    public KafkaEventTemplate(KafkaConfig kafkaConfig, ProducerFactory<K, V> producerFactory) {
        super(producerFactory);
        this.kafkaConfig = kafkaConfig;
    }

    public CompletableFuture<SendResult<K, V>> send(@Nullable V data) {
        return super.send(kafkaConfig.getKafkaTopic(), data);
    }
}
