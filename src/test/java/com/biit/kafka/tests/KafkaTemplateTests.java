package com.biit.kafka.tests;

import com.biit.kafka.events.KafkaEventTemplate;
import com.biit.kafka.events.consumers.TestTemplateEventConsumerListeners;
import com.biit.kafka.events.entities.TestEvent;
import com.biit.kafka.events.entities.TestPayload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

@SpringBootTest
@EnableKafka
@Test(groups = {"kafkaTemplateEvents"})
public class KafkaTemplateTests extends AbstractTestNGSpringContextTests {
    private static final int EVENTS_QUANTITY = 100;

    @Autowired
    private KafkaEventTemplate<String, TestEvent> kafkaTemplate;

    @Autowired
    private TestTemplateEventConsumerListeners testTemplateEventConsumerListeners;

    private String receivedPayload = null;


    private TestPayload generatePayload(int value) {
        TestPayload testPayload = new TestPayload();
        testPayload.setValue("Event" + value);
        return testPayload;
    }

    @BeforeClass
    public void setListener() {
        testTemplateEventConsumerListeners.addListener(event ->  {
            System.out.println("########################### EVENT RECEIVED ###########################");
            this.receivedPayload = event.getPayload();
        });
    }

    @Test
    public void produceEvents() {
        kafkaTemplate.send(new TestEvent(generatePayload(0)));
        await().atMost(Duration.ofMinutes(3)).untilAsserted(() ->
                Assert.assertNotNull(receivedPayload));
    }
}
