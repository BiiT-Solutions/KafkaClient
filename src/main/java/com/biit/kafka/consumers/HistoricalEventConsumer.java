package com.biit.kafka.consumers;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class HistoricalEventConsumer<T> {
    private static final int MAX_EMPTY_POLLING = 5;
    private static final int POLLING_DURATION = 100;

    private final KafkaConfig kafkaConfig;
    private final Class<T> type;
    private Duration pollingDuration;
    private boolean running;
    private Thread thread;
    private final Set<EventReceivedListener<T>> eventReceivedListeners = new HashSet<>();
    private final ObjectMapper objectMapper;

    public interface EventReceivedListener<T> {
        void received(T entity);
    }

    public HistoricalEventConsumer(Class<T> type, KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.type = type;
        objectMapper = new ObjectMapper();
    }


    /**
     * Starts a consumer with a custom list of topics.
     *
     * @param topics kafka topics.
     */
    public void startConsumer(Collection<String> topics) {
        startConsumer(topics, LocalDateTime.now());
    }

    /**
     * Starts a consumer with a custom list of topics, reading from a starting point in time.
     *
     * @param topics       kafka topics.
     * @param startingTime starting point in time.
     */
    public void startConsumer(Collection<String> topics, LocalDateTime startingTime) {
        this.startConsumer(topics, startingTime, null);
    }

    /**
     * Starts a consumer with a custom list of topics, reading from a starting point in time and a specific duration.
     * For obtaining the results, subscribe to the EventReceivedListener listeners.
     *
     * @param topics       kafka topics.
     * @param startingTime starting point in time.
     * @param duration     the duration.
     */
    public void startConsumer(Collection<String> topics, LocalDateTime startingTime, Duration duration) {
        if (getThread() != null) {
            throw new UnsupportedOperationException("Kafka consumer thread already running");
        }

        final Map<String, Object> properties = new HashMap<>(kafkaConfig.getProperties());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (org.apache.kafka.clients.consumer.Consumer<?, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            if (startingTime != null) {
                final List<TopicPartition> topicPartitions = new ArrayList<>(topics.size());
                final Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
                for (final String topic : topics) {
                    final TopicPartition topicPartition = new TopicPartition(topic, 0);
                    timestampsToSearch.put(topicPartition, Timestamp.valueOf(startingTime).getTime());
                    topicPartitions.add(topicPartition);
                }
                kafkaConsumer.assign(topicPartitions);
                final Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes;
                if (duration != null) {
                    offsetsForTimes = kafkaConsumer.offsetsForTimes(timestampsToSearch, duration);
                } else {
                    offsetsForTimes = kafkaConsumer.offsetsForTimes(timestampsToSearch);
                }
                if (offsetsForTimes != null && !offsetsForTimes.isEmpty()) {
                    for (final Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
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
            final Thread thread = new Thread(() -> {
                try {
                    while (isRunning()) {
                        final ConsumerRecords<?, String> consumerRecords = kafkaConsumer.poll(pollingDuration);
                        for (final ConsumerRecord<?, String> record : consumerRecords) {
                            try {
                                sendEventReceivedListener(objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL).readValue(record.value(), type));
                            } catch (IOException e) {
                                KafkaLogger.errorMessage(this.getClass().getName(), e);
                            }
                        }
                    }
                } catch (Exception e) {
                    KafkaLogger.errorMessage(this.getClass().getName(), e);
                } finally {
                    kafkaConsumer.close();
                    stopConsumer();
                }
            });
            thread.setDaemon(true);
            setThread(thread);
            thread.start();
        }
    }

    /**
     * Retrieves a list of events synchronously. Users spring boot properties for the topics.
     *
     * @param startingTime starting point in time.
     * @param duration     the duration.
     * @return a collection of events.
     */
    public Collection<T> getEvents(LocalDateTime startingTime, Duration duration) {
        return getEvents(Collections.singletonList(kafkaConfig.getKafkaTopic()), startingTime, duration);
    }

    /**
     * Retrieves a list of events synchronously.
     *
     * @param topics       kafka topics.
     * @param startingTime starting point in time.
     * @param duration     the duration.
     * @return a collection of events.
     */
    public Collection<T> getEvents(Collection<String> topics, LocalDateTime startingTime, Duration duration) {
        final List<T> result = new ArrayList<>();
        final Map<String, Object> properties = new HashMap<>(kafkaConfig.getProperties());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        final org.apache.kafka.clients.consumer.Consumer<?, String> kafkaConsumer = new KafkaConsumer<>(properties);
        final List<TopicPartition> topicPartitions = new ArrayList<>(topics.size());
        final Map<TopicPartition, Long> beginningTimestamps = new HashMap<>();
        final long startingTimeAsMilliseconds = Timestamp.valueOf(startingTime).getTime();
        for (final String topic : topics) {
            final TopicPartition topicPartition = new TopicPartition(topic, 0);
            beginningTimestamps.put(topicPartition, startingTimeAsMilliseconds);
            topicPartitions.add(topicPartition);
        }
        kafkaConsumer.assign(topicPartitions);
        final Map<TopicPartition, OffsetAndTimestamp> beginningOffsets;
        if (duration != null) {
            beginningOffsets = kafkaConsumer.offsetsForTimes(beginningTimestamps, duration);
        } else {
            beginningOffsets = kafkaConsumer.offsetsForTimes(beginningTimestamps);
        }
        if (beginningOffsets != null && !beginningOffsets.isEmpty()) {
            for (final Map.Entry<TopicPartition, OffsetAndTimestamp> entry : beginningOffsets.entrySet()) {
                if (entry.getValue() == null) {
                    kafkaConsumer.seekToEnd(Collections.singleton(entry.getKey()));
                } else {
                    kafkaConsumer.seek(entry.getKey(), entry.getValue().offset());
                }
            }
        }

        int consecutiveEmptyPollings = 0;
        while (consecutiveEmptyPollings < MAX_EMPTY_POLLING) {
            final ConsumerRecords<?, String> consumerRecords = kafkaConsumer.poll(getPollingDuration());
            if (!consumerRecords.isEmpty()) {
                for (ConsumerRecord<?, String> record : consumerRecords) {
                    try {
                        result.add(objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL).readValue(record.value(), type));
                    } catch (IOException e) {
                        KafkaLogger.errorMessage(this.getClass().getName(), e);
                    }
                }
                consecutiveEmptyPollings = 0;
            } else {
                consecutiveEmptyPollings++;
            }
        }
        kafkaConsumer.close();
        stopConsumer();
        return result;
    }

    public void addEventReceivedListener(EventReceivedListener<T> listener) {
        eventReceivedListeners.add(listener);
    }

    private void sendEventReceivedListener(T eventEntity) {
        eventReceivedListeners.forEach(listener -> listener.received(eventEntity));
    }

    public Duration getPollingDuration() {
        if (pollingDuration == null) {
            return Duration.ofMillis(POLLING_DURATION);
        }
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
