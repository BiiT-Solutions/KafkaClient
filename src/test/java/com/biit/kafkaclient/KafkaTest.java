package com.biit.kafkaclient;

import com.biit.eventstructure.event.KeyEvent;
import com.biit.kafkaclient.valuesofinterest.BasicEvent;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

@Test(groups = {"clientLocal"})
public class KafkaTest {

    public static final String TOPIC_NAME = "test3";
    public static final int EVENTS_QUANTITY = 100;

    @Test
    public synchronized void interfaceTest() throws InterruptedException {
        IKafkaConsumerClient<BasicEvent> kafkaClient = new KafkaConsumerClient<>(BasicEvent.class);
        Set<BasicEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<BasicEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        kafkaClient.startConsumer(Collections.singleton(TOPIC_NAME), consumerEvents::add);
        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            BasicEvent generatedEvent = EventGenerator.generateNewBasicEvent();
            producerEvents.add(generatedEvent);
            KafkaProducerClient.send(TOPIC_NAME, generatedEvent);
        }
        wait(getWaitingTime());
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    public synchronized void multipleProducerTest() throws InterruptedException {
        IKafkaConsumerClient<BasicEvent> kafkaClient = new KafkaConsumerClient<>(BasicEvent.class);
        Set<BasicEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY * 2));
        Set<BasicEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        Set<BasicEvent> producerEvents2 = new HashSet<>(EVENTS_QUANTITY);
        kafkaClient.startConsumer(Collections.singleton(TOPIC_NAME), consumerEvents::add);
        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            BasicEvent generatedEvent = EventGenerator.generateNewBasicEvent();
            producerEvents.add(generatedEvent);
            KafkaProducerClient.send(TOPIC_NAME, generatedEvent);
            BasicEvent generatedEvent2 = EventGenerator.generateNewBasicEvent();
            producerEvents2.add(generatedEvent2);
            KafkaProducerClient.send(TOPIC_NAME, generatedEvent2);
        }
        producerEvents.addAll(producerEvents2);
        wait(getWaitingTime());
        Assert.assertEquals(consumerEvents, producerEvents);
    }

    public synchronized void multipleConsumerTest() throws InterruptedException {
        IKafkaConsumerClient<BasicEvent> kafkaClient = new KafkaConsumerClient<>(BasicEvent.class);
        Set<BasicEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        IKafkaConsumerClient<BasicEvent> kafkaClient2 = new KafkaConsumerClient<>(BasicEvent.class);
        Set<BasicEvent> consumerEvents2 = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<BasicEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        kafkaClient.startConsumer(Collections.singleton(TOPIC_NAME), consumerEvents::add);
        kafkaClient2.startConsumer(Collections.singleton(TOPIC_NAME), consumerEvents2::add);

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            BasicEvent generatedEvent = EventGenerator.generateNewBasicEvent();
            producerEvents.add(generatedEvent);
            KafkaProducerClient.send(TOPIC_NAME, generatedEvent);
        }
        wait(getWaitingTime());
        Assert.assertEquals(consumerEvents, producerEvents);
        Assert.assertEquals(consumerEvents2, consumerEvents);
    }

    public synchronized void simulationTest() throws InterruptedException {
        Calendar initialDate = Calendar.getInstance();
        initialDate.set(2016, Calendar.FEBRUARY, 1);
        long initialTimestamp = initialDate.getTimeInMillis();
        Calendar finalDate = Calendar.getInstance();
        finalDate.set(2016, Calendar.MAY, 1);
        long finalTimestamp = finalDate.getTimeInMillis();

        IKafkaConsumerClient<BasicEvent> kafkaClient = new KafkaConsumerClient<>(BasicEvent.class);
        Set<BasicEvent> consumerEvents = Collections.synchronizedSet(new HashSet<>(EVENTS_QUANTITY));
        Set<BasicEvent> producerEvents = new HashSet<>(EVENTS_QUANTITY);
        kafkaClient.startConsumer(Collections.singleton(TOPIC_NAME), basicEvent -> {
            if (basicEvent.getCreationTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() >=
                    initialTimestamp && basicEvent.getCreationTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() <= finalTimestamp) {
                consumerEvents.add(basicEvent);
            }
        });

        for (int i = 0; i < EVENTS_QUANTITY; i++) {
            BasicEvent eventInRange = EventGenerator.generateNewBasicEvent(initialTimestamp, finalTimestamp);
            producerEvents.add(eventInRange);
            KafkaProducerClient.send(TOPIC_NAME, eventInRange);
            KafkaProducerClient.send(TOPIC_NAME, EventGenerator.generateNewBasicEvent());
        }
        wait(getWaitingTime());
        Assert.assertEquals(consumerEvents, producerEvents);
    }


    private static final int DEMO_PROCESS_ID = 22506;

    @Test
    public synchronized void sendDemoKeyEvents() throws InterruptedException {
        List<KeyEvent> eventsToSend = new ArrayList<>(10);
        LocalDateTime past = LocalDateTime.now().minusDays(9);
        eventsToSend.add(new KeyEvent(1 + "", past, 0, 0, DEMO_PROCESS_ID, "projectStarted", "designACampaign"));
        past = past.plusDays(2);
        eventsToSend.add(new KeyEvent(2 + "", past, 0, 0, DEMO_PROCESS_ID, "pspi", 0.5));
        eventsToSend.add(new KeyEvent(3 + "", past.plusHours(4), 0, 0, DEMO_PROCESS_ID, "delayedPeriod", "1 day"));
        eventsToSend.add(new KeyEvent(4 + "", past.plusHours(8), 0, 0, DEMO_PROCESS_ID, "flag", "yellow"));
        past = past.plusDays(5);
        eventsToSend.add(new KeyEvent(5 + "", past, 0, 0, DEMO_PROCESS_ID, "pspi", 0.5));
        eventsToSend.add(new KeyEvent(6 + "", past.plusHours(8), 0, 0, DEMO_PROCESS_ID, "delayedPeriod", "1 day"));
        eventsToSend.add(new KeyEvent(7 + "", past.plusHours(4), 0, 0, DEMO_PROCESS_ID, "flag", "yellow"));
        past = past.plusDays(1);
        eventsToSend.add(new KeyEvent(8 + "", past, 0, 0, DEMO_PROCESS_ID, "pspi", 0.4));
        eventsToSend.add(new KeyEvent(9 + "", past.plusHours(4), 0, 0, DEMO_PROCESS_ID, "delayedPeriod", "2 days"));
        eventsToSend.add(new KeyEvent(10 + "", past.plusHours(8), 0, 0, DEMO_PROCESS_ID, "flag", "yellow"));
        for (KeyEvent keyEvent : eventsToSend) {
            KafkaProducerClient.send("keyEvents", keyEvent);
        }
        wait(5000);
    }

    private int getWaitingTime() {
        return Math.max(4 * EVENTS_QUANTITY, 2000);
    }
}
