package com.biit.kafkaclient;

import com.biit.eventstructure.event.IKafkaStorable;
import com.biit.kafkaclient.config.KafkaClientConfigurationReader;
import com.biit.kafkaclient.logger.KafkaClientLogger;
import com.biit.rest.serialization.JacksonSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

public class KafkaConsumerClient<V extends IKafkaStorable> implements IKafkaConsumerClient<V> {

	private final Class<V> type;

	private Properties properties;
	private Duration pollingDuration;
	private boolean running;
	private Thread thread;

	public KafkaConsumerClient(Class<V> type) {
		this.type = type;
		setProperties(KafkaClientConfigurationReader.getInstance().getAllPropertiesAsPropertiesClass());
		setPollingDuration(Duration.ofMillis(1000));
		setThread(null);
	}

	@Override
	public void startConsumer(Collection<String> topics, Consumer<V> consumer) {
		startConsumer(topics, consumer, new Date());
	}

	@Override
	public void startConsumer(Collection<String> topics, Consumer<V> callback, Date startingTime) {
		this.startConsumer(topics, callback, startingTime, null);
	}

	@Override
	public void startConsumer(Collection<String> topics, Consumer<V> callback, Date startingTime, Duration duration) {
		if (getThread() != null) {
			throw new UnsupportedOperationException("Kafka consumer thread already running");
		}

		org.apache.kafka.clients.consumer.Consumer<?, String> kafkaConsumer = new KafkaConsumer(getProperties());
		if (startingTime != null) {
			List<TopicPartition> topicPartitions = new ArrayList<>(topics.size());
			Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
			for (String topic : topics) {
				TopicPartition topicPartition = new TopicPartition(topic, 0);
				timestampsToSearch.put(topicPartition, startingTime.getTime());
				topicPartitions.add(topicPartition);
			}
			kafkaConsumer.assign(topicPartitions);
			Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes;
			if (duration != null) {
				offsetsForTimes = kafkaConsumer.offsetsForTimes(timestampsToSearch, duration);
			} else {
				offsetsForTimes = kafkaConsumer.offsetsForTimes(timestampsToSearch);
			}
			if (offsetsForTimes != null && !offsetsForTimes.isEmpty()) {
				for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
					if (entry.getValue() == null) {
						kafkaConsumer.seekToEnd(Collections.singleton(entry.getKey()));
					} else {
						kafkaConsumer.seek(entry.getKey(), entry.getValue().offset());
					}
				}
			}
		} else {
			kafkaConsumer.subscribe(topics);
		}

		this.running = true;
		Thread thread = new Thread(() -> {
			try {
				while (isRunning()) {
					final ConsumerRecords<?, String> consumerRecords = kafkaConsumer.poll(pollingDuration);
					for (ConsumerRecord record : consumerRecords) {
						try {
							this.getClass().getTypeParameters();
							callback.accept(JacksonSerializer.getDefaultSerializer().readValue(record.value().toString(), type));
						} catch (IOException e) {
							KafkaClientLogger.errorMessage(this.getClass().getName(), e);
						}
					}
				}
			} catch (Exception e) {
				KafkaClientLogger.errorMessage(this.getClass().getName(), e);
			} finally {
				kafkaConsumer.close();
				stopConsumer();
			}
		});
		thread.setDaemon(true);
		setThread(thread);
		thread.start();
	}

	@Override
	public Properties getProperties() {
		return properties;
	}

	@Override
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	public Duration getPollingDuration() {
		return pollingDuration;
	}

	public void setPollingDuration(Duration pollingDuration) {
		this.pollingDuration = pollingDuration;
	}

	public boolean isRunning() {
		return running;
	}

	public void stopConsumer() {
		this.running = false;
		setThread(null);
	}

	private Thread getThread() {
		return thread;
	}

	private void setThread(Thread thread) {
		this.thread = thread;
	}
}
