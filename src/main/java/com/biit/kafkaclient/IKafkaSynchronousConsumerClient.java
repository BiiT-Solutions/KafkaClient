package com.biit.kafkaclient;

import java.util.Collection;
import java.util.Properties;

public interface IKafkaSynchronousConsumerClient<T> {
	Collection<T> getMessages(long from, long to, Properties properties, Collection<String> topics);

	Collection<T> getMessages(long from, long to, Collection<String> topics);

	Collection<T> getMessages(long from, Collection<String> topics);
}
