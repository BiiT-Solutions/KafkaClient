package com.biit.kafka;

import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;

public class EventSerializer<T> implements Serializer<T> {
    public static final String DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss.SSS";
    public static final LocalDateTimeSerializer LOCAL_DATETIME_SERIALIZER =
            new LocalDateTimeSerializer(DateTimeFormatter.ofPattern(DATETIME_FORMAT));

    private ObjectMapper objectMapper;

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            final JavaTimeModule module = new JavaTimeModule();
            module.addSerializer(LOCAL_DATETIME_SERIALIZER);
            objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).registerModule(module);
        }
        return objectMapper;
    }

    @Override
    public byte[] serialize(String s, T event) {
        try {
            return getObjectMapper().writeValueAsString(event).getBytes(StandardCharsets.UTF_8);
        } catch (JsonProcessingException e) {
            KafkaLogger.errorMessage(this.getClass(), e);
        }
        return new byte[0];
    }
}
