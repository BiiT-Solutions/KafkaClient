package com.biit.kafka.events;

import com.biit.kafka.config.ObjectMapperFactory;
import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;

public class ElementPayload<DTO> implements EventPayload {
    private String json;

    public ElementPayload(DTO dto) {
        try {
            setJson(ObjectMapperFactory.getObjectMapper().writeValueAsString(dto));
        } catch (JsonProcessingException e) {
            KafkaLogger.errorMessage(this.getClass(), e);
            throw new RuntimeException(e);
        }
    }

    public String getJson() {
        return json;
    }

    public void setJson(String json) {
        this.json = json;
    }
}
