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

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.events.Event;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class TemplateEventConsumer<V extends Event<?>> {
    private final KafkaConfig kafkaConfig;
    private final Class<V> type;


    public TemplateEventConsumer(Class<V> type, KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.type = type;
    }

    @Bean
    public ConsumerFactory<String, V> typeConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(kafkaConfig.getProperties(),
                new StringDeserializer(),
                new JsonDeserializer<>(type));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, V> templateEventListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(typeConsumerFactory());
        return factory;
    }
}
