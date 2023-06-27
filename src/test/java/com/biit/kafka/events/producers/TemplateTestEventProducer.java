package com.biit.kafka.events.producers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.producers.TemplateEventProducer;
import org.springframework.beans.factory.annotation.Autowired;

//@Component
public class TemplateTestEventProducer extends TemplateEventProducer {

    @Autowired
    public TemplateTestEventProducer(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

}
