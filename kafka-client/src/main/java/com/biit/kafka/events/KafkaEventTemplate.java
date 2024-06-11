package com.biit.kafka.events;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.logger.KafkaLogger;
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
        KafkaLogger.info(this.getClass(), "KafkaEventTemplate started....");
    }

    public CompletableFuture<SendResult<String, Event>> send(Object data) {
        if (data == null) {
            KafkaLogger.warning(this.getClass(), "Receiving null data.");
            return CompletableFuture.completedFuture(null);
        }
        if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
            KafkaLogger.info(this.getClass(), "Sending data '{}' to topic '{}'.", data, kafkaConfig.getKafkaTopic());
            return super.send(kafkaConfig.getKafkaTopic(), new Event(data));
        }
        KafkaLogger.warning(this.getClass(), "No topic defined.");
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<SendResult<String, Event>> send(@Nullable Event data) {
        if (data == null) {
            KafkaLogger.warning(this.getClass(), "Receiving null data.");
            return CompletableFuture.completedFuture(null);
        }
        if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
            KafkaLogger.info(this.getClass(), "Sending event '{}' to topic '{}'.", data, kafkaConfig.getKafkaTopic());
            return super.send(kafkaConfig.getKafkaTopic(), data);
        }
        KafkaLogger.warning(this.getClass(), "No topic defined.");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<SendResult<String, Event>> send(@Nullable String topic, @Nullable Event data) {
        if (data == null) {
            KafkaLogger.warning(this.getClass(), "Receiving null data.");
            return CompletableFuture.completedFuture(null);
        }
        if (topic == null) {
            if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
                KafkaLogger.info(this.getClass(), "Sending event '{}' to topic '{}'.", data, kafkaConfig.getKafkaTopic());
                return super.send(kafkaConfig.getKafkaTopic(), data);
            }
            KafkaLogger.warning(this.getClass(), "No topic defined.");
            return CompletableFuture.completedFuture(null);
        } else {
            KafkaLogger.info(this.getClass(), "Sending event '{}' to topic '{}'.", data, topic);
            return super.send(topic, data);
        }
    }

    public CompletableFuture<SendResult<String, Event>> send(String topic, String key, Integer partition, Long timestamp, Object data) {
        if (data == null) {
            KafkaLogger.warning(this.getClass(), "Receiving null data.");
            return CompletableFuture.completedFuture(null);
        }
        KafkaLogger.info(this.getClass(), "Sending event '{}' to topic '{}'.", data, topic);
        return send(topic, key, partition, timestamp, new Event(data));
    }


    public CompletableFuture<SendResult<String, Event>> send(String topic, String key, Integer partition, Long timestamp, Event data) {
        if (data == null) {
            KafkaLogger.warning(this.getClass(), "Receiving null data.");
            return CompletableFuture.completedFuture(null);
        }
        if (topic == null || topic.isBlank()) {
            if (kafkaConfig != null && kafkaConfig.getKafkaTopic() != null) {
                topic = kafkaConfig.getKafkaTopic();
            } else {
                KafkaLogger.warning(this.getClass(), "No topic defined.");
                return CompletableFuture.completedFuture(null);
            }
        }
        KafkaLogger.info(this.getClass(), "Sending event '{}' to topic '{}'.", data, topic);
        return super.send(topic, partition, timestamp, key, data);
    }
}
