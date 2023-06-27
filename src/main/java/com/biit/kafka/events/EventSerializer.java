package com.biit.kafka.events;

import com.biit.kafka.config.ObjectMapperFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EventSerializer implements Serializer<Event> {


    @Override
    public byte[] serialize(String topic, Event data) {
        try {
            return ObjectMapperFactory.getObjectMapper().writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Can't serialize object: " + data, e);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }
}
