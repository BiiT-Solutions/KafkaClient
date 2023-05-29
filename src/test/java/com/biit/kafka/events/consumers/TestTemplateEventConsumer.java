package com.biit.kafka.events.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.consumers.TemplateEventConsumer;
import com.biit.kafka.events.entities.TestEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class TestTemplateEventConsumer extends TemplateEventConsumer<TestEvent> {

    @Autowired
    public TestTemplateEventConsumer(KafkaConfig kafkaConfig) {
        super(TestEvent.class, kafkaConfig);
    }
}
