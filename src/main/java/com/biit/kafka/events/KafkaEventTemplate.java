package com.biit.kafka.events;

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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Component
public class KafkaEventTemplate<K, V extends Event<?>> extends KafkaTemplate<K, V> {
    private final KafkaConfig kafkaConfig;

    public KafkaEventTemplate(KafkaConfig kafkaConfig, ProducerFactory<K, V> producerFactory) {
        super(producerFactory);
        this.kafkaConfig = kafkaConfig;
    }

    public ListenableFuture<SendResult<K, V>> send(@Nullable V data) {
        return super.send(kafkaConfig.getKafkaTopic(), data);
    }
}
