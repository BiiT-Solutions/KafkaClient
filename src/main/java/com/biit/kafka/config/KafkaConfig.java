package com.biit.kafka.config;

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

import com.biit.kafka.exceptions.FailedEventDeserializer;
import com.biit.kafka.logger.KafkaLogger;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * Generates the configuration used later by the consumer / producers.
 */
@Configuration
public class KafkaConfig {
    private static final String MAX_FETCH_SIZE = "20971520"; //20MB

    @Value("${spring.kafka.topic:}")
    private String kafkaTopic;

    @Value("${spring.kafka.bootstrap-servers:}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.client.id:}")
    private String kafkaClientId;

    @Value("${spring.kafka.group.id:}")
    private String kafkaGroupId;

    @Value("${spring.kafka.key.serializer:}")
    private String kafkaKeySerializer;

    @Value("${spring.kafka.value.serializer:}")
    private String kafkaValueSerializer;

    @Value("${spring.kafka.key.deserializer:}")
    private String kafkaKeyDeserializer;

    @Value("${spring.kafka.value.deserializer:}")
    private String kafkaValueDeserializer;

    private final SecureRandom secureRandom = new SecureRandom();

    public Map<String, Object> getProperties() {
        final Map<String, Object> props = new HashMap<>();
        if (kafkaBootstrapServers != null && !kafkaBootstrapServers.isEmpty()) {
            KafkaLogger.debug(this.getClass().getName(), "Connecting to Kafka server '" + kafkaBootstrapServers + "'");
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        }
        if (kafkaClientId != null && !kafkaClientId.isEmpty()) {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ID" + kafkaClientId);
        } else {
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, "ID" + Math.abs(secureRandom.nextInt(Integer.MAX_VALUE)));
        }
        if (kafkaGroupId != null && !kafkaGroupId.isEmpty()) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "ID" + kafkaGroupId);
        }
        if (kafkaKeyDeserializer != null && !kafkaKeyDeserializer.isEmpty()) {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaKeyDeserializer);
        } else {
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        }
        if (kafkaValueDeserializer != null && !kafkaValueDeserializer.isEmpty()) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaValueDeserializer);
        } else {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        }
        if (kafkaKeySerializer != null && !kafkaKeySerializer.isEmpty()) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaKeySerializer);
        } else {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        }
        if (kafkaValueSerializer != null && !kafkaValueSerializer.isEmpty()) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaValueSerializer);
        } else {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        }
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, MAX_FETCH_SIZE);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, MAX_FETCH_SIZE);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_FUNCTION, FailedEventDeserializer.class);
        return props;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public void setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
    }

    public String getKafkaClientId() {
        return kafkaClientId;
    }

    public void setKafkaClientId(String kafkaClientId) {
        this.kafkaClientId = kafkaClientId;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public void setKafkaGroupId(String kafkaGroupId) {
        this.kafkaGroupId = kafkaGroupId;
    }

    public void setKafkaKeySerializer(String kafkaKeySerializer) {
        this.kafkaKeySerializer = kafkaKeySerializer;
    }

    public void setKafkaValueSerializer(String kafkaValueSerializer) {
        this.kafkaValueSerializer = kafkaValueSerializer;
    }

    public void setKafkaKeyDeserializer(String kafkaKeyDeserializer) {
        this.kafkaKeyDeserializer = kafkaKeyDeserializer;
    }

    public void setKafkaValueDeserializer(String kafkaValueDeserializer) {
        this.kafkaValueDeserializer = kafkaValueDeserializer;
    }

    /**
     * With AdminClient from Kafka, the topics are generated programmatically as a bean.
     *
     * @return
     */
    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name(kafkaTopic)
                .partitions(10)
                .replicas(2)
                .build();
    }

}
