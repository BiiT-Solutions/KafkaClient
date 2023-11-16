package com.biit.kafka;

import com.biit.kafka.config.KafkaConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfiguration {

    private static final int PARTITIONS = 10;
    private static final int REPLICAS = 1;


    /**
     * With AdminClient from Kafka, the topics are generated programmatically as a bean.
     *
     * @return The Topic.
     */
    @Bean
    public NewTopic createTestTopic(@Value("${spring.kafka.topic:}") String testTopic) {
        if (testTopic == null) {
            testTopic = KafkaConfig.DEFAULT_TOPIC;
        }
        return TopicBuilder.name(testTopic)
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

}
