package com.biit.kafka.consumers;

/*-
 * #%L
 * Kafka client
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

import com.biit.kafka.events.Event;
import com.biit.kafka.logger.KafkaLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import java.util.Date;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@ConditionalOnExpression("${spring.kafka.enabled:false}")
//@ConditionalOnProperty(prefix = "spring", name = "kafka.enabled", havingValue = "true", matchIfMissing = false)
@EnableKafka
@Configuration
public class EventListener {
    private final Set<EventReceivedListener> listeners;

    @Value("${spring.application.name:#{null}}")
    private String applicationName;

    @Value("#{new Boolean('${kafka.ignore.own.events:true}')}")
    private boolean ignoreOwnEvents;

    public interface EventReceivedListener {
        void received(Event event, Integer offset, String groupId, String key, int partition, String topic, long timeStamp);
    }

    public EventListener() {
        super();
        this.listeners = new HashSet<>();
    }

    public void addListener(EventReceivedListener listener) {
        listeners.add(listener);
    }

    public Set<EventReceivedListener> getListeners() {
        return listeners;
    }

    //These annotations are ignored. Here as future reference only. Use them on the child class.
    @KafkaListener(topics = "${spring.kafka.topic:#{null}}",
            clientIdPrefix = "#{'${spring.kafka.client.id}'?:T(java.util.UUID).randomUUID().toString()}",
            groupId = "#{'${spring.kafka.group.id}'?:T(java.util.UUID).randomUUID().toString()}",
            containerFactory = "templateEventListenerContainerFactory", autoStartup = "${spring.kafka.enabled:false}")
    public void eventsListener(@Payload(required = false) Event event,
                               final @Header(KafkaHeaders.OFFSET) Integer offset,
                               final @Header(KafkaHeaders.GROUP_ID) String groupId,
                               final @Header(value = KafkaHeaders.KEY, required = false) String key,
                               final @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
        if (!ignoreOwnEvents || !Objects.equals(event.getReplyTo(), applicationName)) {
            if (event != null) {
                KafkaLogger.debug(this.getClass().getName(), "Event received with topic '{}', key '{}',"
                                + " offset '{}', group '{}', on partition '{}' received at '{}' with content:\n'{}'.",
                        topic, key, offset, groupId, partition, new Date(timeStamp), event.toString());
                listeners.forEach(eventReceivedListener -> eventReceivedListener.received(event, offset, key, groupId, partition, topic, timeStamp));
            } else {
                KafkaLogger.warning(this.getClass(), "Null event received with topic '{}', key '{}',"
                                + " offset '{}', group '{}', on partition '{}' received at '{}'",
                        topic, key, offset, groupId, partition, new Date(timeStamp));
            }
        } else {
            KafkaLogger.debug(this.getClass(), "Ignoring event from '{}' on topic '{}', key '{}',"
                            + " offset '{}', group '{}', on partition '{}' received at '{}' as is an event from this application.",
                    event.getReplyTo(), topic, key, offset, groupId, partition, new Date(timeStamp));
        }
    }
}
