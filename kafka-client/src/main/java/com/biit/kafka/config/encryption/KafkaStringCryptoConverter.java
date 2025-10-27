package com.biit.kafka.config.encryption;

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

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter(autoApply = true)
public class KafkaStringCryptoConverter extends KafkaAbstractCryptoConverter<String> implements AttributeConverter<String, String> {

    @Override
    protected boolean isNotNullOrEmpty(String attribute) {
        return attribute != null && !attribute.isEmpty();
    }

    @Override
    protected String stringToEntityAttribute(String dbData) {
        return dbData;
    }

    @Override
    protected String entityAttributeToString(String attribute) {
        return attribute;
    }
}
