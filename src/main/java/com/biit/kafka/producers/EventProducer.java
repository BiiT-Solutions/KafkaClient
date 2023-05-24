package com.biit.kafka.producers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.logger.KafkaLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public abstract class EventProducer<E> {

    private final KafkaConfig kafkaConfig;
    private KafkaTemplate<String, E> eventTemplate;

    @Value("${kafka.topic:}")
    private String kafkaTopic;

    public EventProducer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }


    public void sendEvent(E event) {
        final ListenableFuture<SendResult<String, E>> future = getEventTemplate().send(kafkaTopic, event);
        future.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onSuccess(SendResult<String, E> result) {
                if (result != null) {
                    KafkaLogger.debug(this.getClass(), "Sent event '{}' with offset '{}'.", event,
                            result.getRecordMetadata().offset());
                } else {
                    KafkaLogger.warning(this.getClass(), "Sent event '{}' with no result.", event);
                }
            }

            @Override
            public void onFailure(Throwable ex) {
                KafkaLogger.severe(this.getClass(), "Unable to send event '{}' due to '{}'", event,
                        ex.getMessage());
            }
        });
    }

    protected KafkaTemplate<String, E> getEventTemplate() {
        if (eventTemplate == null) {
            eventTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(kafkaConfig.getProperties()));
        }
        return eventTemplate;
    }

}
