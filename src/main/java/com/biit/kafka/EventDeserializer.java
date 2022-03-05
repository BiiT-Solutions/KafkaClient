package com.biit.kafka;

import com.biit.kafka.logger.KafkaLogger;
import com.biit.kafka.security.CipherInitializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.biit.kafka.security.EncryptionConfiguration.eventEncryptionKey;

public class EventDeserializer<T> implements Deserializer<T> {
    public static final LocalDateTimeDeserializer LOCAL_DATETIME_SERIALIZER =
            new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(EventSerializer.DATETIME_FORMAT));

    private ObjectMapper objectMapper;
    private Class<T> clazz;

    private static Cipher cipher;

    protected EventDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            final JavaTimeModule module = new JavaTimeModule();
            module.addDeserializer(LocalDateTime.class, LOCAL_DATETIME_SERIALIZER);
            objectMapper = Jackson2ObjectMapperBuilder.json()
                    .modules(module)
                    .featuresToDisable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    .build();
        }
        return objectMapper;
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        try {
            return getObjectMapper().readValue(new String(decrypt(bytes), StandardCharsets.UTF_8), clazz);
        } catch (IllegalArgumentException | JsonProcessingException e) {
            KafkaLogger.debug(this.getClass(), "Not a valid event.");
        }
        return null;
    }

    private static Cipher getCipher() throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, NoSuchPaddingException, InvalidKeyException {
        if (cipher == null) {
            final CipherInitializer cipherInitializer = new CipherInitializer();
            cipher = cipherInitializer.prepareAndInitCipher(Cipher.DECRYPT_MODE, eventEncryptionKey);
        }
        return cipher;
    }

    public byte[] decrypt(byte[] data) {
        if (eventEncryptionKey != null && !eventEncryptionKey.isEmpty() && data != null && data.length > 0) {
            try {
                KafkaLogger.debug(this.getClass(), "Event decrypted!");
                return getCipher().doFinal(data);
            } catch (NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException | BadPaddingException | NoSuchPaddingException
                    | IllegalBlockSizeException e) {
                throw new RuntimeException(e);
            }
        }
        return data;
    }
}
