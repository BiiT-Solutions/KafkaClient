package com.biit.kafka;

import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class EventDeserializer<T> implements Deserializer<T> {
    public static final LocalDateTimeDeserializer LOCAL_DATETIME_SERIALIZER =
            new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(EventSerializer.DATETIME_FORMAT));

    private ObjectMapper objectMapper;
    private Class<T> clazz;

    protected EventDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            final JavaTimeModule module = new JavaTimeModule();
            module.addDeserializer(LocalDateTime.class, LOCAL_DATETIME_SERIALIZER);
            objectMapper = Jackson2ObjectMapperBuilder.json()
                    .modules(module)
                    .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .build();
        }
        return objectMapper;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        try {
            return getObjectMapper().readValue(new String(bytes, StandardCharsets.UTF_8), clazz);
        } catch (IllegalArgumentException | JsonProcessingException e) {
            KafkaLogger.debug(this.getClass(), "Not a valid event.");
        }
        return null;
    }
}
