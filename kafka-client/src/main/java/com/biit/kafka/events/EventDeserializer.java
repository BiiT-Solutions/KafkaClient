package com.biit.kafka.events;

import com.biit.cipher.CipherInitializer;
import com.biit.kafka.config.ObjectMapperFactory;
import com.biit.kafka.logger.KafkaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;

import static com.biit.cipher.EncryptionConfiguration.encryptionKey;

@Component
public class EventDeserializer implements Deserializer<Event> {
    public static final String DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss.SSS";
    public static final LocalDateTimeDeserializer LOCAL_DATETIME_SERIALIZER =
            new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(DATETIME_FORMAT));

    private ObjectMapper objectMapper;

    protected TypeReference<Event> getJsonParser() {
        return new TypeReference<>() {
        };
    }

    @Override
    public Event deserialize(String topic, byte[] bytes) {
        try {
            if (KafkaLogger.isDebugEnabled()) {
                KafkaLogger.debug(this.getClass(), "Received event '{}' -> '{}'", byteArrayToHex(bytes),
                        new String(bytes, StandardCharsets.UTF_8));
            }
            final String data = new String(bytes, StandardCharsets.UTF_8);
            return ObjectMapperFactory.getObjectMapper().readValue(data, getJsonParser());
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
                final byte[] decryptedData = CipherInitializer.getCipherForDecrypt().doFinal(data);
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
        final StringBuilder stringBuilder = new StringBuilder(a.length * 2);
        for (byte b : a) {
            stringBuilder.append(String.format("%02x", b));
        }
        return stringBuilder.toString();
    }
}

