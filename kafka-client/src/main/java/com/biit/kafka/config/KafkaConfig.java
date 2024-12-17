package com.biit.kafka.config;

import com.biit.kafka.exceptions.FailedEventDeserializer;
import com.biit.kafka.logger.KafkaLogger;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import javax.annotation.PostConstruct;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Generates the configuration used later by the consumer / producers.
 */
@Configuration
@ConditionalOnExpression("${spring.kafka.enabled:false}")
public class KafkaConfig {
    private static final String MAX_FETCH_SIZE = "20971520"; //20MB

    public static final String DEFAULT_TOPIC = "DefaultTopic";
    private static final int PARTITIONS = 10;
    private static final int REPLICAS = 1;
    private static final int LINGER_MS_CONFIG = 5;


    @Value("${spring.kafka.topic:}")
    private String kafkaTopic;

    @Value("${spring.kafka.bootstrap-servers:}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.client.id:}")
    private String kafkaClientId;

    @Value("#{'${spring.kafka.group.id}'?:T(java.util.UUID).randomUUID().toString()}")
    private String kafkaGroupId;

    @Value("${spring.kafka.producer.key-serializer:}")
    private String kafkaKeySerializer;

    @Value("${spring.kafka.producer.value-serializer:}")
    private String kafkaValueSerializer;

    @Value("${spring.kafka.consumer.key-deserializer:}")
    private String kafkaKeyDeserializer;

    @Value("${spring.kafka.consumer.value-deserializer:}")
    private String kafkaValueDeserializer;

    @Value("${spring.kafka.number.retries:}")
    private String kafkaNumberOfRetries;

    @Value("${spring.kafka.delivery.timeout:}")
    private String kafkaDeliveryTimeout;

    @Value("${spring.kafka.request.timeout:}")
    private String kafkaRequestTimeout;

    @Value("${spring.kafka.max.request.size:5242880}")
    private String kafkaMessageMaxBytes;

    @Value("${spring.kafka.producer.compression-type:snappy}")
    private String kafkaCompressionType;

    private final SecureRandom secureRandom = new SecureRandom();

    public KafkaConfig() {

    }

    @PostConstruct
    public void created() {
        KafkaLogger.debug(this.getClass(), "KafkaConfig started with:\n{}", this);
    }

    public Map<String, Object> getConsumerProperties() {
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
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "GR" + kafkaGroupId);
        } else {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "GR" + UUID.randomUUID());
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
        if (kafkaRequestTimeout != null && !kafkaRequestTimeout.isEmpty()) {
            props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
        }
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, kafkaMessageMaxBytes != null ? kafkaMessageMaxBytes : MAX_FETCH_SIZE);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, kafkaMessageMaxBytes != null ? kafkaMessageMaxBytes : MAX_FETCH_SIZE);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_FUNCTION, FailedEventDeserializer.class);
        return props;
    }

    public Map<String, Object> getProducerProperties() {
        final Map<String, Object> props = new HashMap<>();
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
        if (kafkaNumberOfRetries != null && !kafkaNumberOfRetries.isEmpty()) {
            props.put(ProducerConfig.RETRIES_CONFIG, kafkaNumberOfRetries);
        }
        if (kafkaRequestTimeout != null && !kafkaRequestTimeout.isEmpty()) {
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, kafkaDeliveryTimeout);
        }

        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaMessageMaxBytes != null ? kafkaMessageMaxBytes : MAX_FETCH_SIZE);
        props.put("buffer.memory", kafkaMessageMaxBytes != null ? kafkaMessageMaxBytes : MAX_FETCH_SIZE);
        props.put("message.max.bytes", kafkaMessageMaxBytes != null ? kafkaMessageMaxBytes : MAX_FETCH_SIZE);
        props.put("max.message.bytes", kafkaMessageMaxBytes != null ? kafkaMessageMaxBytes : MAX_FETCH_SIZE);

        if (kafkaCompressionType != null && !kafkaCompressionType.isEmpty()) {
            //If sending json payloads, snappy compression is recommended. But on alpine is needed 'apk add --no-cache gcompat'
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaCompressionType);
            //https://developer.ibm.com/articles/benefits-compression-kafka-messaging/
            props.put(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG);
            //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        }
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
        return TopicBuilder.name(Objects.requireNonNullElse(kafkaTopic, DEFAULT_TOPIC))
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Override
    public String toString() {
        return "KafkaConfig{"
                + "kafkaTopic='" + kafkaTopic + '\''
                + ", kafkaBootstrapServers='" + kafkaBootstrapServers + '\''
                + ", kafkaClientId='" + kafkaClientId + '\''
                + ", kafkaGroupId='" + kafkaGroupId + '\''
                + ", kafkaKeySerializer='" + kafkaKeySerializer + '\''
                + ", kafkaValueSerializer='" + kafkaValueSerializer + '\''
                + ", kafkaKeyDeserializer='" + kafkaKeyDeserializer + '\''
                + ", kafkaValueDeserializer='" + kafkaValueDeserializer + '\''
                + ", kafkaNumberOfRetries='" + kafkaNumberOfRetries + '\''
                + ", kafkaDeliveryTimeout='" + kafkaDeliveryTimeout + '\''
                + ", kafkaRequestTimeout='" + kafkaRequestTimeout + '\''
                + '}';
    }
}
