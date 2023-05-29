package com.biit.kafka.events.producers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.entities.TestEvent;
import com.biit.kafka.producers.TemplateEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TemplateTestEventProducer extends TemplateEventProducer<TestEvent> {

    @Autowired
    public TemplateTestEventProducer(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

}
