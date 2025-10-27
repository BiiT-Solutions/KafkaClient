package com.biit.kafka.events;

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

import com.biit.kafka.logger.EventsLogger;
import com.biit.server.controllers.models.ElementDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;

import java.time.LocalDateTime;
import java.util.UUID;

public class EventSender<DTO> implements IEventSender<DTO> {

    @Value("${spring.kafka.send.topic:}")
    private String sendTopic;

    @Value("${spring.application.name:#{null}}")
    private String applicationName;

    @Value("#{new Boolean('${spring.kafka.enabled:false}')}")
    private boolean kafkaEnabled;

    private final KafkaEventTemplate kafkaTemplate;

    private final String factType;
    private final String tag;

    public EventSender(@Autowired(required = false) KafkaEventTemplate kafkaTemplate, String tag, String factType) {
        this.kafkaTemplate = kafkaTemplate;
        this.factType = factType;
        this.tag = tag;
    }

    public boolean isKafkaEnabled() {
        return kafkaEnabled;
    }

    @Override
    public void sendEvents(DTO dto, EventSubject subject, String executedBy) {
        sendEvents(dto, subject, tag, executedBy);
    }

    public void sendEvents(DTO dto, EventSubject subject, String tag, String executedBy) {
        if (kafkaEnabled && kafkaTemplate != null) {
            EventsLogger.debug(this.getClass().getName(), "Preparing for sending events on topic '{}'...", sendTopic);
            if (sendTopic != null && !sendTopic.isEmpty()) {
                //Send the complete form as an event.
                kafkaTemplate.send(sendTopic, getEvent(dto, subject, tag, executedBy));
                EventsLogger.debug(this.getClass().getName(), "Event '{}' with dto '{}' send by '{}' on tag '{}'!", subject, dto, executedBy, tag);
            } else {
                EventsLogger.warning(this.getClass().getName(), "Send topic not defined!");
            }
        }
    }

    private Event getEvent(DTO dto, EventSubject subject, String tag, String createdBy) {
        if (dto == null) {
            return null;
        }
        final Event event = new Event(dto);
        if (createdBy != null) {
            event.setCreatedBy(createdBy);
        } else if (dto instanceof ElementDTO<?>) {
            event.setCreatedBy(((ElementDTO<?>) dto).getCreatedBy());
        }
        event.setMessageId(UUID.randomUUID());
        if (subject != null) {
            event.setSubject(subject.toString());
        }
        event.setContentType(MediaType.APPLICATION_JSON_VALUE);
        event.setCreatedAt(LocalDateTime.now());
        event.setReplyTo(applicationName);
        event.setTag(tag);
        event.setCustomProperty(EventCustomProperties.FACT_TYPE, factType);
        return event;
    }
}
