package com.biit.kafkaclient;

import com.biit.kafkaclient.config.KafkaClientConfigurationReader;
import com.biit.kafkaclient.logger.KafkaClientLogger;
import com.biit.rest.serialization.JacksonSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public interface IKafkaProducerClient {
	Producer<?, String> PRODUCER = new KafkaProducer<>(KafkaClientConfigurationReader.getInstance().getAllPropertiesAsPropertiesClass());

	static void send(String topic, Object value) {
		try {
			PRODUCER.send(new ProducerRecord<>(topic, JacksonSerializer.getDefaultSerializer().writeValueAsString(value)));
		} catch (JsonProcessingException e) {
			KafkaClientLogger.errorMessage(IKafkaProducerClient.class, e);
		}
	}
}
