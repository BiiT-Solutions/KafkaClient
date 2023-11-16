package com.biit.kafka.events;

import com.biit.kafka.logger.EventsLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;

import java.time.LocalDateTime;
import java.util.UUID;

public class EventSender<DTO> implements IEventSender<DTO> {

    @Value("${spring.kafka.send.topic:}")
    private String sendTopic;

    @Value("${spring.application.name:#{null}}")
    private String applicationName;

    private final KafkaEventTemplate kafkaTemplate;

    private final String factType;

    public EventSender(KafkaEventTemplate kafkaTemplate, String factType) {
        this.kafkaTemplate = kafkaTemplate;
        this.factType = factType;
    }


    @Override
    public void sendEvents(DTO dto, EventSubject subject, String executedBy) {
        EventsLogger.debug(this.getClass().getName(), "Preparing for sending events...");
        if (sendTopic != null && !sendTopic.isEmpty()) {
            //Send the complete form as an event.
            kafkaTemplate.send(sendTopic, getEvent(dto, subject, executedBy));
            EventsLogger.debug(this.getClass().getName(), "Event '{}' with dto '{}' send by '{}'!", subject, dto, executedBy);
        } else {
            EventsLogger.warning(this.getClass().getName(), "Send topic not defined!");
        }
    }

    private Event getEvent(DTO dto, EventSubject subject, String createdBy) {
        final Event event = new Event(new ElementPayload<>(dto));
        event.setCreatedBy(createdBy);
        event.setMessageId(UUID.randomUUID());
        if (subject != null) {
            event.setSubject(subject.toString());
        }
        event.setContentType(MediaType.APPLICATION_JSON_VALUE);
        event.setCreatedAt(LocalDateTime.now());
        event.setReplyTo(applicationName);
        event.setTag("Recurrence");
        event.setCustomProperty(EventCustomProperties.FACT_TYPE, factType);
        return event;
    }
}
