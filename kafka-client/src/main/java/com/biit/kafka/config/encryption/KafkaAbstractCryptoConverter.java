package com.biit.kafka.config.encryption;

import com.biit.database.encryption.ICipherEngine;
import com.biit.database.encryption.InvalidEncryptionException;
import com.biit.database.encryption.logger.EncryptorLogger;
import jakarta.persistence.AttributeConverter;

import static com.biit.kafka.config.encryption.KafkaKeyProperty.getEncryptionKey;

public abstract class KafkaAbstractCryptoConverter<T> implements AttributeConverter<T, String> {

    private final ICipherEngine cipherEngine;

    public KafkaAbstractCryptoConverter() {
        this(KafkaAbstractCryptoConverter.generateEngine());
    }

    public KafkaAbstractCryptoConverter(ICipherEngine cipherEngine) {
        this.cipherEngine = cipherEngine;
    }

    public static ICipherEngine generateEngine() {
        return new KafkaCBCCipherEngine();
    }

    @Override
    public String convertToDatabaseColumn(T attribute) {
        if (getEncryptionKey() != null && !getEncryptionKey().isEmpty() && isNotNullOrEmpty(attribute)) {
            try {
                return encrypt(attribute);
            } catch (InvalidEncryptionException e) {
                throw new RuntimeException(e);
            }
        }
        return entityAttributeToString(attribute);
    }

    @Override
    public T convertToEntityAttribute(String dbData) {
        if (getEncryptionKey() != null && !getEncryptionKey().isEmpty() && dbData != null && !dbData.isEmpty()) {
            try {
                return decrypt(dbData);
            } catch (InvalidEncryptionException e) {
                throw new RuntimeException(e);
            }
        }
        return stringToEntityAttribute(dbData);
    }

    protected abstract boolean isNotNullOrEmpty(T attribute);

    protected abstract T stringToEntityAttribute(String dbData);

    protected abstract String entityAttributeToString(T attribute);

    private String encrypt(T attribute) throws InvalidEncryptionException {
        return cipherEngine.encrypt(entityAttributeToString(attribute));
    }

    private T decrypt(String dbData) throws InvalidEncryptionException {
        final T entity = stringToEntityAttribute(cipherEngine.decrypt(dbData));
        EncryptorLogger.debug(this.getClass().getName(), "Decrypted value for '{}' is '{}'.", dbData, entity);
        return entity;
    }

}
