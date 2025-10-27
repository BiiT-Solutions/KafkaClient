package com.biit.kafka.events;

/*-
 * #%L
 * Kafka client
 * %%
 * Copyright (C) 2021 - 2025 BiiT Sourcing Solutions S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
            KafkaLogger.info(this.getClass(), "Sending data '{}' on topic '{}'.", data, kafkaConfig.getKafkaTopic());
            if (!KafkaLogger.isDebugEnabled()) {
                KafkaLogger.info(this.getClass(), "Sending event on topic '{}'.", kafkaConfig.getKafkaTopic());
            } else {
                KafkaLogger.debug(this.getClass(), "Sending event '{}' on topic '{}'.", data, kafkaConfig.getKafkaTopic());
            }
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
            if (!KafkaLogger.isDebugEnabled()) {
                KafkaLogger.info(this.getClass(), "Sending event '{}' on topic '{}'.", data.getMessageId(), kafkaConfig.getKafkaTopic());
            } else {
                KafkaLogger.debug(this.getClass(), "Sending event '{}' on topic '{}'.", data, kafkaConfig.getKafkaTopic());
            }
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
                if (!KafkaLogger.isDebugEnabled()) {
                    KafkaLogger.info(this.getClass(), "Sending event '{}' on topic '{}'.", data.getMessageId(), kafkaConfig.getKafkaTopic());
                } else {
                    KafkaLogger.debug(this.getClass(), "Sending event '{}' on topic '{}'.", data, kafkaConfig.getKafkaTopic());
                }
                return super.send(kafkaConfig.getKafkaTopic(), data);
            }
            KafkaLogger.warning(this.getClass(), "No topic defined.");
            return CompletableFuture.completedFuture(null);
        } else {
            if (!KafkaLogger.isDebugEnabled()) {
                KafkaLogger.info(this.getClass(), "Sending event '{}' on topic '{}'.", data.getMessageId(), topic);
            } else {
                KafkaLogger.debug(this.getClass(), "Sending event '{}' on topic '{}'.", data, topic);
            }
            return super.send(topic, data);
        }
    }

    public CompletableFuture<SendResult<String, Event>> send(String topic, String key, Integer partition, Long timestamp, Object data) {
        if (data == null) {
            KafkaLogger.warning(this.getClass(), "Receiving null data.");
            return CompletableFuture.completedFuture(null);
        }
        if (!KafkaLogger.isDebugEnabled()) {
            KafkaLogger.info(this.getClass(), "Sending event on topic '{}'.", topic);
        } else {
            KafkaLogger.debug(this.getClass(), "Sending event '{}' on topic '{}'.", data, topic);
        }
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
        if (!KafkaLogger.isDebugEnabled()) {
            KafkaLogger.info(this.getClass(), "Sending event '{}' on topic '{}'.", data.getMessageId(), topic);
        } else {
            KafkaLogger.debug(this.getClass(), "Sending event '{}' on topic '{}'.", data, topic);
        }
        return super.send(topic, partition, timestamp, key, data);
    }
}
