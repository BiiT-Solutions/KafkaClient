package com.biit.kafkaclient;

import com.biit.eventstructure.event.KeyEvent;
import com.biit.kafkaclient.valuesofinterest.BasicEvent;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
			if (basicEvent.getCreationTime().getTime() >= initialTimestamp && basicEvent.getCreationTime().getTime() <= finalTimestamp) {
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
		Instant instant = Instant.now().minus(9, ChronoUnit.DAYS);
		eventsToSend.add(new KeyEvent(1 + "", new Date(instant.getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "projectStarted", "designACampaign"));
		instant = instant.plus(2, ChronoUnit.DAYS);
		eventsToSend.add(new KeyEvent(2 + "", new Date(instant.getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "pspi", 0.5));
		eventsToSend.add(new KeyEvent(3 + "", new Date(instant.plus(4, ChronoUnit.HOURS).getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "delayedPeriod", "1 day"));
		eventsToSend.add(new KeyEvent(4 + "", new Date(instant.plus(8, ChronoUnit.HOURS).getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "flag", "yellow"));
		instant = instant.plus(5, ChronoUnit.DAYS);
		eventsToSend.add(new KeyEvent(5 + "", new Date(instant.getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "pspi", 0.5));
		eventsToSend.add(new KeyEvent(6 + "", new Date(instant.plus(8, ChronoUnit.HOURS).getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "delayedPeriod", "1 day"));
		eventsToSend.add(new KeyEvent(7 + "", new Date(instant.plus(4, ChronoUnit.HOURS).getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "flag", "yellow"));
		instant = instant.plus(1, ChronoUnit.DAYS);
		eventsToSend.add(new KeyEvent(8 + "", new Date(instant.getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "pspi", 0.4));
		eventsToSend.add(new KeyEvent(9 + "", new Date(instant.plus(4, ChronoUnit.HOURS).getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "delayedPeriod", "2 days"));
		eventsToSend.add(new KeyEvent(10 + "", new Date(instant.plus(8, ChronoUnit.HOURS).getEpochSecond() * 1000L), 0, 0, DEMO_PROCESS_ID, "flag", "yellow"));
		for (KeyEvent keyEvent : eventsToSend) {
			KafkaProducerClient.send("keyEvents", keyEvent);
		}
		wait(5000);
	}

	private int getWaitingTime() {
		return 4 * EVENTS_QUANTITY > 2000 ? 4 * EVENTS_QUANTITY : 2000;
	}
}
