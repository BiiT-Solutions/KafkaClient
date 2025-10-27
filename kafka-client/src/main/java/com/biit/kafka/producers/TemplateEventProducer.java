package com.biit.kafka.producers;

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
import com.biit.kafka.events.KafkaEventTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

public class TemplateEventProducer {
    private final KafkaConfig kafkaConfig;

    public TemplateEventProducer(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    @Bean
    public ProducerFactory<String, Event> producerFactory() {
        return new DefaultKafkaProducerFactory<>(kafkaConfig.getProducerProperties());
    }

    @Bean
    public KafkaTemplate<String, Event> kafkaTemplate() {
        return new KafkaEventTemplate(kafkaConfig, producerFactory());
    }
}
