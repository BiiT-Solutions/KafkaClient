package com.biit.kafka.consumers;

import com.biit.kafka.logger.KafkaLogger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public abstract class EventListener<T> {
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

    //These annotations are ignored. Here as future reference only. Use them on the child class.
    @KafkaListener(topics = "#{'${spring.kafka.topic}'.split(',')", clientIdPrefix = "#{T(java.util.UUID).randomUUID().toString()}",
            containerFactory = "eventListenerContainerFactory")
    public void eventsListener(T event,
                               final @Header(KafkaHeaders.OFFSET) Integer offset,
                               final @Header(value = KafkaHeaders.KEY, required = false) String key,
                               final @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
        KafkaLogger.debug(this.getClass().getName(), "Event received with topic '{}', key '{}',"
                        + " offset '{}', on partition '{}' received at '{}' with content:\n'{}'.",
                topic, key, offset, partition, new Date(timeStamp), event.toString());
        listeners.forEach(eventReceivedListener -> eventReceivedListener.received(event));
    }
}
