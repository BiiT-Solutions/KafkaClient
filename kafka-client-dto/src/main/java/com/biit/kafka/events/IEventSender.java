package com.biit.kafka.events;

public interface IEventSender<DTO> {

    void sendEvents(DTO dto, EventSubject subject, String executedBy);
}
