package com.biit.kafka.consumers;

import com.biit.kafka.events.Event;
import com.biit.kafka.logger.KafkaLogger;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@EnableKafka
@Configuration
public class EventListener {
    private final Set<EventReceivedListener> listeners;

    public interface EventReceivedListener {
        void received(Event event, Integer offset, String key, int partition, String topic, long timeStamp);
    }

    public EventListener() {
        this.listeners = new HashSet<>();
    }

    public void addListener(EventReceivedListener listener) {
        listeners.add(listener);
    }

    public Set<EventReceivedListener> getListeners() {
        return listeners;
    }

    //These annotations are ignored. Here as future reference only. Use them on the child class.
    @KafkaListener(topics = "${spring.kafka.topic:#{null}}", clientIdPrefix = "#{T(java.util.UUID).randomUUID().toString()}",
            containerFactory = "templateEventListenerContainerFactory")
    public void eventsListener(@Payload(required = false) Event event,
                               final @Header(KafkaHeaders.OFFSET) Integer offset,
                               final @Header(value = KafkaHeaders.KEY, required = false) String key,
                               final @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
        KafkaLogger.debug(this.getClass().getName(), "Event received with topic '{}', key '{}',"
                        + " offset '{}', on partition '{}' received at '{}' with content:\n'{}'.",
                topic, key, offset, partition, new Date(timeStamp), event.toString());
        listeners.forEach(eventReceivedListener -> eventReceivedListener.received(event, offset, key, partition, topic, timeStamp));
    }
}
