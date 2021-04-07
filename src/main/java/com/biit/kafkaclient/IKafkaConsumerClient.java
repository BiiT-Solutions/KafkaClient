package com.biit.kafkaclient;

import com.biit.eventstructure.event.IKafkaStorable;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.function.Consumer;

public interface IKafkaConsumerClient<V extends IKafkaStorable> {

	void setProperties(Properties properties);

	void startConsumer(Collection<String> topics, Consumer<V> consumer);

	void startConsumer(Collection<String> topics, Consumer<V> callback, Date startingTime);

	void startConsumer(Collection<String> topics, Consumer<V> callback, Date startingTime, Duration duration);

	Properties getProperties();
}
