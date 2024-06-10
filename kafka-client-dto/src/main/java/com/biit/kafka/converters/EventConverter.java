package com.biit.kafka.converters;

import com.biit.kafka.config.ObjectMapperFactory;
import com.biit.kafka.controllers.models.EventDTO;
import com.biit.kafka.converters.models.EventConverterRequest;
import com.biit.kafka.events.Event;
import com.biit.server.controller.converters.ElementConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

@Component
public class EventConverter extends ElementConverter<Event, EventDTO, EventConverterRequest> {


    @Override
    protected EventDTO convertElement(EventConverterRequest from) {
        if (from.getEntity() == null) {
            return null;
        }
        final EventDTO eventDTO = new EventDTO();
        BeanUtils.copyProperties(from.getEntity(), eventDTO);
        eventDTO.setMessageId(from.getEntity().getMessageId());
        eventDTO.setSessionId(from.getEntity().getSessionId());
        eventDTO.setCorrelationId(from.getEntity().getCorrelationId());
        try {
            eventDTO.setPayload(ObjectMapperFactory.getObjectMapper().readValue(from.getEntity().getPayload(), new TypeReference<>() {
            }));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return eventDTO;
    }

    @Override
    public Event reverse(EventDTO to) {
        if (to == null) {
            return null;
        }
        final Event event = new Event();
        BeanUtils.copyProperties(to, event);
        event.setMessageId(to.getMessageId());
        event.setSessionId(to.getSessionId());
        event.setCorrelationId(to.getCorrelationId());
        try {
            event.setPayload(ObjectMapperFactory.getObjectMapper().writeValueAsString(to.getPayload()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return event;
    }
}
