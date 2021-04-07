package com.biit.kafkaclient;

import com.biit.eventstructure.event.IKafkaStorable;
import com.biit.kafkaclient.config.KafkaClientConfigurationReader;
import com.biit.kafkaclient.logger.KafkaClientLogger;
import com.biit.rest.serialization.JacksonSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerClient {
	private static Producer<String, String> producer;

	public static void send(String topic, IKafkaStorable value) {
		if (value == null) {
			throw new IllegalArgumentException("value must be not null.");
		} else {
			try {
				getProducer().send(new ProducerRecord<>(topic, null, value.getCreationTime().getTime(), value.getId(),
						JacksonSerializer.getDefaultSerializer().writeValueAsString(value)));
			} catch (JsonProcessingException e) {
				KafkaClientLogger.errorMessage(KafkaProducerClient.class, e);
			}
		}
	}

	private static Producer<String, String> getProducer() {
		if (producer == null) {
			Thread.currentThread().setContextClassLoader(null);
			producer = new KafkaProducer<>(
					KafkaClientConfigurationReader.getInstance().getAllPropertiesAsPropertiesClass());
		}
		return producer;
	}
}
