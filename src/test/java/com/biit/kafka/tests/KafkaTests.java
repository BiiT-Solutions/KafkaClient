package com.biit.kafka.tests;

import com.biit.kafka.testevents.TestEvent;
import com.biit.kafka.testevents.consumers.TestEventConsumer;
import com.biit.kafka.testevents.consumers.TestEventConsumer2;
import com.biit.kafka.testevents.consumers.TestEventConsumerListeners;
import com.biit.kafka.testevents.consumers.TestEventConsumerListeners2;
import com.biit.kafka.testevents.producers.TestEventProducer;
import com.biit.kafka.testevents.producers.TestEventProducer2;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootTest
@Test(groups = {"kafkaEvents"})
public class KafkaTests extends AbstractTestNGSpringContextTests {
    private static final String TOPIC_NAME = "facts";
    private static final int EVENTS_QUANTITY = 100;
    private static final String QUESTION = "Question? ";
    private static final String ANSWER = "Answer: ";


    @Autowired
    private TestEventProducer testEventProducer;

    @Autowired
    private TestEventConsumerListeners testEventConsumerListeners;

    @Autowired
    private TestEventConsumerListeners2 testEventConsumerListeners2;

    @Autowired
    private TestEventProducer2 testEventProducer2;

    @Autowired
    private TestEventConsumer testEventConsumer;

    @Autowired
    private TestEventConsumer2 testEventConsumer2;

    private ObjectMapper objectMapper;

    private TestEvent generateEvent(int value) {
        TestEvent testEvent = new TestEvent();
        testEvent.setValue("Event" + value);
        return testEvent;
    }

    public TestEvent generateEvent(int value, LocalDateTime minTimestamp, LocalDateTime maxTimestamp) {
        TestEvent TestEvent = generateEvent(value);

        //Create a random day.
        // create ZoneId
        ZoneOffset zone = ZoneOffset.of("Z");
        long randomSecond = ThreadLocalRandom.current().nextLong(minTimestamp.toEpochSecond(zone), maxTimestamp.toEpochSecond(zone));
        LocalDateTime randomDate = LocalDateTime.ofEpochSecond(randomSecond, 0, zone);

        TestEvent.setCreatedAt(randomDate);
        return TestEvent;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            final JavaTimeModule module = new JavaTimeModule();
            LocalDateTimeDeserializer localDateTimeDeserializer = new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"));
            module.addDeserializer(LocalDateTime.class, localDateTimeDeserializer);
            objectMapper = Jackson2ObjectMapperBuilder.json()
                    .modules(module)
                    .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .build();
        }
        return objectMapper;
    }

    public synchronized void factTest() throws InterruptedException {
        Set<TestEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        //Store received events into set.
        testEventConsumerListeners.addListener(fact -> consumerEvents.add((TestEvent) fact));

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestEvent generatedEvent = generateEvent(i);
            producerEvents.add(generatedEvent);
            testEventProducer.sendFact(generatedEvent);
        }

        wait(consumerEvents);
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    public synchronized void multipleProducerTest() throws InterruptedException {
        Set<TestEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY * 2));
        Set<TestEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        Set<TestEvent> producerEvents2 = new HashSet<>(EVENTS_QUANTITY);
        testEventConsumerListeners.addListener(fact -> consumerEvents.add((TestEvent) fact));
        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestEvent generatedEvent = generateEvent(i);
            producerEvents.add(generatedEvent);
            testEventProducer.sendFact(generatedEvent);
            TestEvent generatedEvent2 = generateEvent(i);
            producerEvents2.add(generatedEvent2);
            testEventProducer2.sendFact(generatedEvent2);
        }
        producerEvents.addAll(producerEvents2);
        wait(consumerEvents);
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    public synchronized void multipleConsumerTest() throws InterruptedException {
        Set<TestEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestEvent> consumerEvents2 = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        testEventConsumerListeners.addListener(fact -> consumerEvents.add((TestEvent) fact));
        testEventConsumerListeners2.addListener(fact -> consumerEvents2.add((TestEvent) fact));

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestEvent generatedEvent = generateEvent(i);
            producerEvents.add(generatedEvent);
            testEventProducer.sendFact(generatedEvent);
        }
        wait(consumerEvents);
        Assert.assertEquals(consumerEvents, producerEvents);
        Assert.assertEquals(consumerEvents2, producerEvents);
    }

    public synchronized void simulationTest() throws InterruptedException {
        LocalDateTime initialDate = LocalDateTime.of(2022, Calendar.FEBRUARY, 1, 0, 0, 0);
        LocalDateTime finalDate = LocalDateTime.of(2022, Calendar.MAY, 1, 23, 59, 59);

        Set<TestEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        testEventConsumerListeners.addListener(fact -> {
            if (fact.getCreatedAt().isAfter(initialDate) && fact.getCreatedAt().isBefore(finalDate)) {
                consumerEvents.add(fact);
            }
        });

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestEvent eventInRange = generateEvent(i, initialDate, finalDate);
            producerEvents.add(eventInRange);
            testEventProducer.sendFact(eventInRange);
            testEventProducer.sendFact(eventInRange);
        }
        wait(consumerEvents);
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    private int getWaitingTime() {
        return EVENTS_QUANTITY * 40;
    }

    private void wait(Set<TestEvent> consumerEvents) throws InterruptedException {
        int i = 0;
        do {
            wait(1000);
            i++;
        } while (consumerEvents.isEmpty() && i < 5);
    }
}
