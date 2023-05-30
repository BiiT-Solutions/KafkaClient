package com.biit.kafka.tests;

import com.biit.kafka.events.KafkaEventTemplate;
import com.biit.kafka.events.consumers.TestHistoricalEventConsumer;
import com.biit.kafka.events.consumers.TestTemplateEventConsumerListeners;
import com.biit.kafka.events.consumers.TestTemplateEventConsumerListeners2;
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
import java.util.concurrent.atomic.AtomicInteger;

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

    @Autowired
    private TestTemplateEventConsumerListeners2 testTemplateEventConsumerListeners2;

    @Autowired
    private TestHistoricalEventConsumer testHistoricalEventConsumer;

    private TestPayload eventPayload = null;
    private TestPayload eventPayload2 = null;


    private TestPayload generatePayload(int value) {
        TestPayload testPayload = new TestPayload();
        testPayload.setValue("Event" + value);
        return testPayload;
    }

    @BeforeClass
    public void setListener() {
        testTemplateEventConsumerListeners.addListener(event -> this.eventPayload = event.getEntity());
    }

    @BeforeClass
    public void setOtherListener() {
        testTemplateEventConsumerListeners2.addListener(event -> this.eventPayload2 = event.getEntity());
    }

    @Test
    public void produceEvents() {
        kafkaTemplate.send(new TestEvent(generatePayload(0)));

        //Check both listeners read the same event.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Assert.assertNotNull(eventPayload);
            Assert.assertNotNull(eventPayload2);
        });
    }

    //Disable as sometimes fail.
//    @Test(dependsOnMethods = "produceEvents")
//    public void historicalData() {
//        Assert.assertEquals(testHistoricalEventConsumer.getEvents(LocalDateTime.now().minusSeconds(6), Duration.ofHours(1)).size(), 1);
//    }

    @Test(dependsOnMethods = "produceEvents")
    public synchronized void produceMultipleEvents() {
        AtomicInteger eventsReceived1 = new AtomicInteger();
        AtomicInteger eventsReceived2 = new AtomicInteger();

        testTemplateEventConsumerListeners.addListener(event -> eventsReceived1.getAndIncrement());
        testTemplateEventConsumerListeners2.addListener(event -> eventsReceived2.getAndIncrement());

        for (int i = 1; i <= EVENTS_QUANTITY; i++) {
            kafkaTemplate.send(new TestEvent(generatePayload(i)));
        }
        //Check both listeners read the same event.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Assert.assertEquals(eventsReceived1.get(), 100);
            Assert.assertEquals(eventsReceived2.get(), 100);
        });
    }
}
