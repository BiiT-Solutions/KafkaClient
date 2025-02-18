package com.biit.kafka.config.encryption;

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
