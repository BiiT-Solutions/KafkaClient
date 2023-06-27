package com.biit.kafka.events;

import com.biit.kafka.config.KafkaConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class KafkaEventTemplate extends KafkaTemplate<String, Event> {
    private final KafkaConfig kafkaConfig;

    public KafkaEventTemplate(KafkaConfig kafkaConfig, ProducerFactory<String, Event> producerFactory) {
        super(producerFactory);
        this.kafkaConfig = kafkaConfig;
    }

    public CompletableFuture<SendResult<String, Event>> send(EventPayload data) {
        return super.send(kafkaConfig.getKafkaTopic(), new Event(data));
    }

    public CompletableFuture<SendResult<String, Event>> send(@Nullable Event data) {
        return super.send(kafkaConfig.getKafkaTopic(), data);
    }

    public CompletableFuture<SendResult<String, Event>> send(@Nullable String topic, EventPayload data) {
        return send(topic, new Event(data));
    }

    @Override
    public CompletableFuture<SendResult<String, Event>> send(@Nullable String topic, @Nullable Event data) {
        if (topic == null) {
            return super.send(kafkaConfig.getKafkaTopic(), data);
        } else {
            return super.send(topic, data);
        }
    }

    public CompletableFuture<SendResult<String, Event>> send(String topic, String key, Integer partition, Long timestamp, EventPayload data) {
        return send(topic, key, partition, timestamp, new Event(data));
    }


    public CompletableFuture<SendResult<String, Event>> send(String topic, String key, Integer partition, Long timestamp, Event data) {
        if (topic == null || topic.isBlank()) {
            topic = kafkaConfig.getKafkaTopic();
        }
        return super.send(topic, partition, timestamp, key, data);
    }
}
