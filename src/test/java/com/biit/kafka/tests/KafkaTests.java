package com.biit.kafka.tests;

import com.biit.cipher.CipherInitializer;
import com.biit.kafka.events.entities.TestPayload;
import com.biit.kafka.events.consumers.TestEventConsumer;
import com.biit.kafka.events.consumers.TestEventConsumer2;
import com.biit.kafka.events.consumers.TestEventConsumerListeners;
import com.biit.kafka.events.consumers.TestEventConsumerListeners2;
import com.biit.kafka.events.producers.TestEventProducer;
import com.biit.kafka.events.producers.TestEventProducer2;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootTest
@Test(groups = {"kafkaEvents"})
public class KafkaTests extends AbstractTestNGSpringContextTests {
    private static final int EVENTS_QUANTITY = 100;


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

    private TestPayload generateEvent(int value) {
        TestPayload testPayload = new TestPayload();
        testPayload.setValue("Event" + value);
        return testPayload;
    }

    @Test
    public TestPayload generateEvent(int value, LocalDateTime minTimestamp, LocalDateTime maxTimestamp) {
        TestPayload TestPayload = generateEvent(value);

        //Create a random day.
        // create ZoneId
        ZoneOffset zone = ZoneOffset.of("Z");
        long randomSecond = ThreadLocalRandom.current().nextLong(minTimestamp.toEpochSecond(zone), maxTimestamp.toEpochSecond(zone));
        LocalDateTime randomDate = LocalDateTime.ofEpochSecond(randomSecond, 0, zone);

        TestPayload.setCreatedAt(randomDate);
        return TestPayload;
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    @Test
    public void checkDecrypt() throws InvalidKeySpecException, InvalidAlgorithmParameterException, NoSuchAlgorithmException,
            InvalidKeyException, NoSuchPaddingException, BadPaddingException, IllegalBlockSizeException {
        String data = "45c616cae84c09ea460788f6becf12e00464658c5a0701f2de51f5b89f1226e1";
        CipherInitializer.getCipherForDecrypt().doFinal(hexStringToByteArray(data));
    }

    @Test
    public synchronized void factTest() throws InterruptedException {
        Set<TestPayload> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestPayload> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        //Store received events into set.
        testEventConsumerListeners.addListener(consumerEvents::add);

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestPayload generatedEvent = generateEvent(i);
            producerEvents.add(generatedEvent);
            testEventProducer.sendEvent(generatedEvent);
        }

        wait(consumerEvents);
        Assert.assertEquals(consumerEvents.size(), producerEvents.size());
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    @Test
    public synchronized void multipleProducerTest() throws InterruptedException {
        Set<TestPayload> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY * 2));
        Set<TestPayload> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        Set<TestPayload> producerEvents2 = new HashSet<>(EVENTS_QUANTITY);
        testEventConsumerListeners.addListener(consumerEvents::add);
        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestPayload generatedEvent = generateEvent(i);
            producerEvents.add(generatedEvent);
            testEventProducer.sendEvent(generatedEvent);
            TestPayload generatedEvent2 = generateEvent(i);
            producerEvents2.add(generatedEvent2);
            testEventProducer2.sendEvent(generatedEvent2);
        }
        producerEvents.addAll(producerEvents2);
        wait(consumerEvents);
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    @Test
    public synchronized void multipleConsumerTest() throws InterruptedException {
        Set<TestPayload> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestPayload> consumerEvents2 = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestPayload> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        testEventConsumerListeners.addListener(consumerEvents::add);
        testEventConsumerListeners2.addListener(consumerEvents2::add);

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestPayload generatedEvent = generateEvent(i);
            producerEvents.add(generatedEvent);
            testEventProducer.sendEvent(generatedEvent);
        }
        wait(consumerEvents);
        Assert.assertEquals(consumerEvents, producerEvents);
        Assert.assertEquals(consumerEvents2, producerEvents);
    }

    @Test
    public synchronized void simulationTest() throws InterruptedException {
        LocalDateTime initialDate = LocalDateTime.of(2022, Calendar.FEBRUARY, 1, 0, 0, 0);
        LocalDateTime finalDate = LocalDateTime.of(2022, Calendar.MAY, 1, 23, 59, 59);

        Set<TestPayload> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<TestPayload> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        testEventConsumerListeners.addListener(fact -> {
            if (fact.getCreatedAt().isAfter(initialDate) && fact.getCreatedAt().isBefore(finalDate)) {
                consumerEvents.add(fact);
            }
        });

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            TestPayload eventInRange = generateEvent(i, initialDate, finalDate);
            producerEvents.add(eventInRange);
            testEventProducer.sendEvent(eventInRange);
            testEventProducer.sendEvent(eventInRange);
        }
        wait(consumerEvents);
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    private void wait(Set<TestPayload> consumerEvents) throws InterruptedException {
        int i = 0;
        do {
            wait(1000);
            i++;
        } while (consumerEvents.size() < EVENTS_QUANTITY && i < 20);
    }
}
