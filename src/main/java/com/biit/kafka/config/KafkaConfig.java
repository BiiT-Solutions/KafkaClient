package com.biit.kafka.config;

import com.biit.kafka.exceptions.FailedEventDeserializer;
import com.biit.kafka.logger.KafkaLogger;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
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

    @Value("${kafka.topic:}")
    private String kafkaTopic;

    @Value("${kafka.bootstrap.servers:}")
    private String kafkaBootstrapServers;

    @Value("${kafka.client.id:}")
    private String kafkaClientId;

    @Value("${kafka.group.id:}")
    private String kafkaGroupId;

    @Value("${kafka.key.serializer:}")
    private String kafkaKeySerializer;

    @Value("${kafka.value.serializer:}")
    private String kafkaValueSerializer;

    @Value("${kafka.key.deserializer:}")
    private String kafkaKeyDeserializer;

    @Value("${kafka.value.deserializer:}")
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
        }
        if (kafkaValueDeserializer != null && !kafkaValueDeserializer.isEmpty()) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaValueDeserializer);
        }
        if (kafkaKeySerializer != null && !kafkaKeySerializer.isEmpty()) {
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaKeySerializer);
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

    /**
     * With AdminClient from Kafka, the topics are generated programmatically as a bean.
     *
     * @return
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(getProperties());
    }

    @Bean
    public NewTopic createTopic() {
        return new NewTopic(kafkaTopic, 1, (short) 1);
    }

}
