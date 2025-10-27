package com.biit.kafka.converters;

/*-
 * #%L
 * Kafka client DTO
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

import com.biit.kafka.config.ObjectMapperFactory;
import com.biit.kafka.controllers.models.EventDTO;
import com.biit.kafka.converters.models.EventConverterRequest;
import com.biit.kafka.events.Event;
import com.biit.server.controller.converters.SimpleConverter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;

@Component
public class EventDataConverter extends SimpleConverter<Event, EventDTO, EventConverterRequest> {


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
