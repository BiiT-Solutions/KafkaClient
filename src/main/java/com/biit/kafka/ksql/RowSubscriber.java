package com.biit.kafka.ksql;

/*-
 * #%L
 * Kafka client
 * %%
 * Copyright (C) 2021 - 2025 BiiT Sourcing Solutions S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;

public class RowSubscriber<T> implements Subscriber<Row> {
    private ObjectMapper objectMapper;

    private final Class<T> clazz;

    private Subscription subscription;

    public List<T> consumedItems = new ArrayList<>();

    public RowSubscriber(Class<T> clazz) {
        this.clazz = clazz;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public synchronized void onSubscribe(Subscription subscription) {
        KafkaLogger.debug(this.getClass(), "Subscriber is subscribed.");
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public synchronized void onNext(Row row) {
        String jsonString = row.asObject().toJsonString();
        KafkaLogger.debug(this.getClass(), "Row JSON: {}", jsonString);
        try {
            T item = getObjectMapper().readValue(jsonString, this.clazz);
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
        KafkaLogger.debug(this.getClass(), "Query has ended.");
    }
}
