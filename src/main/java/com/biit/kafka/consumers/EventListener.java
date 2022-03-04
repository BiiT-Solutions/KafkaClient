package com.biit.kafka.consumers;

import com.biit.kafka.logger.KafkaLogger;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.HashSet;
import java.util.Set;

public class EventListener<T> {
    private final Set<EventReceivedListener<T>> listeners;

    public interface EventReceivedListener<T> {
        void received(T event);
    }

    public EventListener() {
        this.listeners = new HashSet<>();
    }

    public void addListener(EventReceivedListener<T> listener) {
        listeners.add(listener);
    }

    public Set<EventReceivedListener<T>> getListeners() {
        return listeners;
    }

    @KafkaListener(topics = "${kafka.topic}", clientIdPrefix = "#{T(java.util.UUID).randomUUID().toString()}", containerFactory = "eventListenerContainerFactory")
    public void eventsListener(T event) {
        KafkaLogger.debug(this.getClass().getName(), "Event received '{}'.", event.toString());
        listeners.forEach(eventReceivedListener -> eventReceivedListener.received(event));
    }
}
