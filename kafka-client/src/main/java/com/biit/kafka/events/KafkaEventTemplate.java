package com.biit.kafka.events;

import com.biit.kafka.config.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class KafkaEventTemplate extends KafkaTemplate<String, Event> {
    private final KafkaConfig kafkaConfig;
    private static final boolean KAFKA_AUTO_FLUSH = true;

    public KafkaEventTemplate(@Autowired(required = false) KafkaConfig kafkaConfig, ProducerFactory<String, Event> producerFactory) {
        super(producerFactory, KAFKA_AUTO_FLUSH);
        this.kafkaConfig = kafkaConfig;
    }

    public CompletableFuture<SendResult<String, Event>> send(EventPayload data) {
        if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
            return super.send(kafkaConfig.getKafkaTopic(), new Event(data));
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<SendResult<String, Event>> send(@Nullable Event data) {
        if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
            return super.send(kafkaConfig.getKafkaTopic(), data);
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<SendResult<String, Event>> send(@Nullable String topic, EventPayload data) {
        return send(topic, new Event(data));
    }

    @Override
    public CompletableFuture<SendResult<String, Event>> send(@Nullable String topic, @Nullable Event data) {
        if (topic == null) {
            if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
                return super.send(kafkaConfig.getKafkaTopic(), data);
            }
            return CompletableFuture.completedFuture(null);
        } else {
            return super.send(topic, data);
        }
    }

    public CompletableFuture<SendResult<String, Event>> send(String topic, String key, Integer partition, Long timestamp, EventPayload data) {
        return send(topic, key, partition, timestamp, new Event(data));
    }


    public CompletableFuture<SendResult<String, Event>> send(String topic, String key, Integer partition, Long timestamp, Event data) {
        if (topic == null || topic.isBlank()) {
            if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
                topic = kafkaConfig.getKafkaTopic();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
        return super.send(topic, partition, timestamp, key, data);
    }
}
