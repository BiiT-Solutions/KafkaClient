package com.biit.kafka.events;

import com.biit.kafka.logger.EventsLogger;
import com.biit.server.controllers.models.ElementDTO;
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

    public EventSender(KafkaEventTemplate kafkaTemplate, String tag, String factType) {
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
        if (kafkaEnabled) {
            EventsLogger.debug(this.getClass().getName(), "Preparing for sending events...");
            if (sendTopic != null && !sendTopic.isEmpty()) {
                //Send the complete form as an event.
                kafkaTemplate.send(sendTopic, getEvent(dto, subject, tag, executedBy));
                EventsLogger.debug(this.getClass().getName(), "Event '{}' with dto '{}' send by '{}'!", subject, dto, executedBy);
            } else {
                EventsLogger.warning(this.getClass().getName(), "Send topic not defined!");
            }
        }
    }

    private Event getEvent(DTO dto, EventSubject subject, String tag, String createdBy) {
        final Event event = new Event(new ElementPayload<>(dto));
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
