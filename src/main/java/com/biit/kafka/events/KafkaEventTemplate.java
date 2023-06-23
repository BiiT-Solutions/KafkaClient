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

    @Override
    public CompletableFuture<SendResult<K, V>> send(@Nullable String topic, @Nullable V data) {
        if (topic == null) {
            return super.send(kafkaConfig.getKafkaTopic(), data);
        } else {
            return super.send(topic, data);
        }
    }

    public CompletableFuture<SendResult<K, V>> send(@Nullable String topic, @Nullable K key, @Nullable Integer partition,
                                                    @Nullable Long timestamp, @Nullable V data) {
        if (topic == null || topic.isBlank()) {
            topic = kafkaConfig.getKafkaTopic();
        }
        if (key == null) {
            return super.send(topic, data);
        } else if (partition == null) {
            return super.send(topic, key, data);
        } else if (timestamp == null) {
            return super.send(topic, partition, key, data);
        }
        return super.send(topic, partition, timestamp, key, data);
    }
}
