package com.biit.kafka.consumers;

import com.biit.cipher.CipherInitializer;
import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.stereotype.Component;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.biit.cipher.EncryptionConfiguration.encryptionKey;

@Component
public class EventDeserializer<T> implements Deserializer<T> {
    public static final String DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss.SSS";
    public static final LocalDateTimeDeserializer LOCAL_DATETIME_SERIALIZER =
            new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(DATETIME_FORMAT));

    private ObjectMapper objectMapper;

    protected TypeReference<T> getJsonParser() {
        return new TypeReference<>() {
        };
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
            if (KafkaLogger.isDebugEnabled()) {
                KafkaLogger.debug(this.getClass(), "Received event '{}' -> '{}'", byteArrayToHex(bytes),
                        new String(bytes, StandardCharsets.UTF_8));
            }
            String data = new String(decrypt(bytes), StandardCharsets.UTF_8);
            return getObjectMapper().readValue(data, getJsonParser());
        } catch (IllegalArgumentException | JsonProcessingException e) {
            KafkaLogger.debug(this.getClass(), "Not a valid event.");
        }
        return null;
    }

    public byte[] decrypt(byte[] data) {
        if (encryptionKey != null && !encryptionKey.isEmpty() && data != null && data.length > 0) {
            try {
                if (KafkaLogger.isDebugEnabled()) {
                    KafkaLogger.debug(this.getClass(), "For decrypt '{}'.", byteArrayToHex(data));
                }
                byte[] decryptedData = CipherInitializer.getCipherForDecrypt().doFinal(data);
                if (KafkaLogger.isDebugEnabled()) {
                    KafkaLogger.debug(this.getClass(), "Decrypted '{}'.", byteArrayToHex(decryptedData));
                }
                return decryptedData;
            } catch (BadPaddingException | IllegalBlockSizeException e) {
                CipherInitializer.resetCipherForEncrypt();
                KafkaLogger.severe(this.getClass(), "Decrypt failed from source '{}'.", byteArrayToHex(data));
                throw new RuntimeException(e);
            }
        }
        return data;
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

