package com.biit.kafka.testevents.consumers;

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.testevents.TestEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@Configuration
public class TestEventConsumerListeners extends EventListener<TestEvent> {
}
