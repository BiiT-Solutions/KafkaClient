package com.biit.kafka.producers;

import com.biit.kafka.KafkaConfig;
import com.biit.kafka.logger.KafkaLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public abstract class EventProducer<T> {

    private final KafkaConfig kafkaConfig;
    private KafkaTemplate<String, T> eventTemplate;

    @Value("${kafka.topic:}")
    private String kafkaTopic;

    public EventProducer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }


    public void sendFact(T fact) {
        final ListenableFuture<SendResult<String, T>> future = getEventTemplate().send(kafkaTopic, fact);
        future.addCallback(new ListenableFutureCallback<SendResult<String, T>>() {

            @Override
            public void onSuccess(SendResult<String, T> result) {
                if (result != null) {
                    KafkaLogger.debug(this.getClass(), "Sent fact '{}' with offset '{}'.", fact,
                            result.getRecordMetadata().offset());
                } else {
                    KafkaLogger.warning(this.getClass(), "Sent fact '{}' with no result.", fact);
                }
            }

            @Override
            public void onFailure(Throwable ex) {
                KafkaLogger.severe(this.getClass(), "\"Unable to send fact '{}' due to '{}'", fact,
                        ex.getMessage());
            }
        });
    }

    protected KafkaTemplate<String, T> getEventTemplate() {
        if (eventTemplate == null) {
            eventTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaConfig.getProperties()));
        }
        return eventTemplate;
    }

}
