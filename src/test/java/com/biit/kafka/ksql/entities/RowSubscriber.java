package com.biit.kafka.ksql.entities;

import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

public class RowSubscriber<T> implements Subscriber<Row> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Class<T> clazz;

    private Subscription subscription;

    public List<T> consumedItems = new ArrayList<>();

    public RowSubscriber(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        KafkaLogger.info(this.getClass(), "Subscriber is subscribed.");
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public synchronized void onNext(Row row) {
        String jsonString = row.asObject().toJsonString();
        KafkaLogger.info(this.getClass(), "Row JSON: {}", jsonString);
        try {
            T item = OBJECT_MAPPER.readValue(jsonString, this.clazz);
            KafkaLogger.info(this.getClass(), "Item: {}", item);
            consumedItems.add(item);
        } catch (JsonProcessingException e) {
            KafkaLogger.severe(this.getClass(), "Unable to parse json", e);
        }

        // Request the next row
        subscription.request(1);
    }

    @Override
    public synchronized void onError(Throwable t) {
        KafkaLogger.severe(this.getClass(), "Received an error", t);
    }

    @Override
    public synchronized void onComplete() {
        KafkaLogger.info(this.getClass(), "Query has ended.");
    }
}