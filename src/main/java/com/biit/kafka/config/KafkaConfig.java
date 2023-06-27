package com.biit.kafka.config;

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
    private static final int PARTITIONS = 10;
    private static final int REPLICAS = 1;


    @Value("${spring.kafka.topic:}")
    private String kafkaTopic;

    @Value("${spring.kafka.bootstrap-servers:}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.client.id:}")
    private String kafkaClientId;

    @Value("${spring.kafka.group.id:}")
    private String kafkaGroupId;

    @Value("${spring.kafka.producer.key-serializer:}")
    private String kafkaKeySerializer;

    @Value("${spring.kafka.producer.value-serializer:}")
    private String kafkaValueSerializer;

    @Value("${spring.kafka.consumer.key-deserializer:}")
    private String kafkaKeyDeserializer;

    @Value("${spring.kafka.consumer.value-deserializer:}")
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
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

}
