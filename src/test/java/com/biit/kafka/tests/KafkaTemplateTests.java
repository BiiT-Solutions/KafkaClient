package com.biit.kafka.tests;

import com.biit.kafka.consumers.EventListener;
import com.biit.kafka.events.KafkaEventTemplate;
import com.biit.kafka.events.consumers.TestHistoricalEventConsumer;
import com.biit.kafka.events.entities.TestEventPayload;
import com.biit.kafka.events.entities.TestEventPayload2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;

@SpringBootTest
@EnableKafka
@Test(groups = {"kafkaTemplateEvents"})
public class KafkaTemplateTests extends AbstractTestNGSpringContextTests {
    private static final int EVENTS_QUANTITY = 100;

    @Autowired
    private KafkaEventTemplate kafkaTemplate;

    @Autowired
    private EventListener eventListener;

    @Autowired
    private TestHistoricalEventConsumer testHistoricalEventConsumer;

    private TestEventPayload eventPayload = null;
    private TestEventPayload2 eventPayload2 = null;


    private TestEventPayload generatePayload(int value) {
        TestEventPayload testPayload = new TestEventPayload();
        testPayload.setValue("Event" + value);
        return testPayload;
    }

    private TestEventPayload2 generatePayload2(int value) {
        TestEventPayload2 testPayload = new TestEventPayload2();
        testPayload.setValue("Event" + value);
        return testPayload;
    }

    @BeforeClass
    public void setListener() {
        eventListener.addListener((event, offset, key, partition, topic, timeStamp) -> {
            if (Objects.equals(event.getEntityType(), TestEventPayload.class.getName())) {
                this.eventPayload = event.getEntity(TestEventPayload.class);
            }
        });
    }

    @BeforeClass
    public void setOtherListener() {
        eventListener.addListener((event, offset, key, partition, topic, timeStamp) -> {
            if (Objects.equals(event.getEntityType(), TestEventPayload2.class.getName())) {
                this.eventPayload2 = event.getEntity(TestEventPayload2.class);
            }
        });
    }

    @Test
    public void produceEvents1() {
        kafkaTemplate.send(generatePayload(0));

        //Check both listeners read the same event.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Assert.assertNotNull(eventPayload);
            Assert.assertNull(eventPayload2);
        });
    }

    @Test(dependsOnMethods = "produceEvents1")
    public void produceEvents2() {
        kafkaTemplate.send(generatePayload2(0));

        //Check both listeners read the same event.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> Assert.assertNotNull(eventPayload2));
    }

    //Disable as sometimes fail.
//    @Test(dependsOnMethods = "produceEvents")
//    public void historicalData() {
//        Assert.assertEquals(testHistoricalEventConsumer.getEvents(LocalDateTime.now().minusSeconds(6), Duration.ofHours(1)).size(), 1);
//    }

    @Test(dependsOnMethods = {"produceEvents1", "produceEvents2"})
    public synchronized void produceMultipleEvents() {
        AtomicInteger eventsReceived1 = new AtomicInteger();
        AtomicInteger eventsReceived2 = new AtomicInteger();

        eventListener.addListener((event, offset, key, partition, topic, timeStamp) -> eventsReceived1.getAndIncrement());
        eventListener.addListener((event, offset, key, partition, topic, timeStamp) -> eventsReceived2.getAndIncrement());

        for (int i = 1; i <= EVENTS_QUANTITY; i++) {
            kafkaTemplate.send(generatePayload(i));
        }
        //Check both listeners read the same event.
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            Assert.assertEquals(eventsReceived1.get(), 100);
            Assert.assertEquals(eventsReceived2.get(), 100);
        });
    }
}
