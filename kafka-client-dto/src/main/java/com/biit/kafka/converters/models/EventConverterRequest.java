package com.biit.kafka.converters.models;

import com.biit.kafka.events.Event;
import com.biit.server.converters.models.ConverterRequest;

public class EventConverterRequest extends ConverterRequest<Event> {
    public EventConverterRequest(Event event) {
        super(event);
    }
}
