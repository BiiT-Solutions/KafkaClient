package com.biit.kafkaclient;

import com.biit.kafkaclient.config.KafkaClientConfigurationReader;
import com.biit.rest.serialization.JacksonSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class KafkaSynchronousConsumerClient<T> implements IKafkaSynchronousConsumerClient<T> {

	private final Class<T> type;
	private Duration pollingDuration;
	private int maxEmptyPollings;

	public KafkaSynchronousConsumerClient(Class<T> tClass) {
		this.type = tClass;
		setMaxEmptyPollings(5);
		setPollingDuration(Duration.ofMillis(100));
	}

	@Override
	public Collection<T> getMessages(long from, long to, Properties properties, Collection<String> topics) {
		List<T> result = new ArrayList<>();
		org.apache.kafka.clients.consumer.Consumer<?, String> kafkaConsumer = new KafkaConsumer(KafkaClientConfigurationReader.getInstance().getAllPropertiesAsPropertiesClass());
		List<TopicPartition> topicPartitions = new ArrayList<>(topics.size());
		Map<TopicPartition, Long> beginningTimestamps = new HashMap<>();
		for (String topic : topics) {
			TopicPartition topicPartition = new TopicPartition(topic, 0);
			beginningTimestamps.put(topicPartition, from);
			topicPartitions.add(topicPartition);
		}
		kafkaConsumer.assign(topicPartitions);
		Map<TopicPartition, OffsetAndTimestamp> beginningOffsets;
		beginningOffsets = kafkaConsumer.offsetsForTimes(beginningTimestamps);
		if (beginningOffsets != null && !beginningOffsets.isEmpty()) {
			for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : beginningOffsets.entrySet()) {
				if (entry.getValue() == null) {
					kafkaConsumer.seekToEnd(Collections.singleton(entry.getKey()));
				} else {
					kafkaConsumer.seek(entry.getKey(), entry.getValue().offset());
				}
			}
		}
		int consecutiveEmptyPollings = 0;
		try {
			while (consecutiveEmptyPollings < getMaxEmptyPollings()) {
				final ConsumerRecords<?, String> consumerRecords = kafkaConsumer.poll(getPollingDuration());
				if (!consumerRecords.isEmpty()) {
					for (ConsumerRecord record : consumerRecords) {
						if (record.timestamp() >= from && record.timestamp() <= to) {
							result.add(JacksonSerializer.getDefaultSerializer().readValue(record.value().toString(), getType()));
						}
					}
					consecutiveEmptyPollings = 0;
				} else {
					consecutiveEmptyPollings++;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public Collection<T> getMessages(long from, long to, Collection<String> topics) {
		return getMessages(from, to, KafkaClientConfigurationReader.getInstance().getAllPropertiesAsPropertiesClass(), topics);
	}

	@Override
	public Collection<T> getMessages(long from, Collection<String> topics) {
		return getMessages(from, new Date().getTime(), topics);
	}

	public Duration getPollingDuration() {
		return pollingDuration;
	}

	public void setPollingDuration(Duration pollingDuration) {
		this.pollingDuration = pollingDuration;
	}

	public int getMaxEmptyPollings() {
		return maxEmptyPollings;
	}

	public void setMaxEmptyPollings(int maxEmptyPollings) {
		this.maxEmptyPollings = maxEmptyPollings;
	}

	public Class<T> getType() {
		return type;
	}
}
