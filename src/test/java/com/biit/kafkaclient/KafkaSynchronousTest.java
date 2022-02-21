package com.biit.kafkaclient;

import com.biit.kafkaclient.valuesofinterest.BasicEvent;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.util.*;

import static com.biit.kafkaclient.EventGenerator.generateNewBasicEvent;

@Test(groups = {"clientLocal"})
public class KafkaSynchronousTest {
	public static final int PREVIOUS_MESSAGES_COUNT = 10;
	public static final int IN_PERIOD_MESSAGES_COUNT = 10;
	public static final String TEST_TOPIC = "keyEvents";


	@Test
	public void getMessagesBetweenDatesTest() {
		IKafkaSynchronousConsumerClient<BasicEvent> client = new KafkaSynchronousConsumerClient<>(BasicEvent.class);
		Set<BasicEvent> expectedResult = new HashSet<>();
		Calendar previousDate = Calendar.getInstance();
		previousDate.set(2016, Calendar.FEBRUARY, 1);
		long previousTimestamp = previousDate.getTimeInMillis();
		Calendar periodStartDate = Calendar.getInstance();
		periodStartDate.set(2016, Calendar.MAY, 1);
		long periodStartTimestamp = periodStartDate.getTimeInMillis();
		System.out.println("Period start: " + periodStartTimestamp);
		Calendar periodEndDate = Calendar.getInstance();
		periodEndDate.set(2016, Calendar.JUNE, 1);
		long periodEndTimestamp = periodEndDate.getTimeInMillis();
		System.out.println("Period start: " + periodEndTimestamp);
		Calendar laterDate = Calendar.getInstance();
		laterDate.set(2017, Calendar.JANUARY, 1);
		long laterTimestamp = laterDate.getTimeInMillis();
		for (int i = 0; i < PREVIOUS_MESSAGES_COUNT; i++) {
			BasicEvent event = generateNewBasicEvent(0, previousTimestamp);
			KafkaProducerClient.send(TEST_TOPIC, event);
			System.out.println("Generated timestamp: " + event.getCreationTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
		}
		for (int i = 0; i < IN_PERIOD_MESSAGES_COUNT; i++) {
			BasicEvent event = generateNewBasicEvent(periodStartTimestamp, periodEndTimestamp);
			expectedResult.add(event);
			KafkaProducerClient.send(TEST_TOPIC, event);
			System.out.println("Generated timestamp: " + event.getCreationTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
		}
		for (int i = 0; i < PREVIOUS_MESSAGES_COUNT; i++) {
			BasicEvent event = generateNewBasicEvent(laterTimestamp, laterTimestamp + 1000);
			KafkaProducerClient.send(TEST_TOPIC, event);
			System.out.println("Generated timestamp: " + event.getCreationTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
		}
		Assert.assertEquals(new HashSet<>(client.getMessages(periodStartTimestamp, periodEndTimestamp, Collections.singleton(TEST_TOPIC))), expectedResult);
	}

	@Test
	public void getTestMessagesTest() {
		IKafkaSynchronousConsumerClient<Object> client = new KafkaSynchronousConsumerClient<>(Object.class);
		Collection<?> messages = client.getMessages(1543750179000L, 12881173929760L, Collections.singleton(TEST_TOPIC));
		for (Object message : messages) {
			System.out.println(message);
		}
	}
}
