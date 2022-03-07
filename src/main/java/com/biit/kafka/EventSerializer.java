package com.biit.kafka;

import com.biit.kafka.logger.KafkaLogger;
import com.biit.kafka.security.CipherInitializer;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import org.apache.kafka.common.serialization.Serializer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.format.DateTimeFormatter;

import static com.biit.kafka.security.EncryptionConfiguration.eventEncryptionKey;

public class EventSerializer<T> implements Serializer<T> {
    public static final String DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss.SSS";
    public static final LocalDateTimeSerializer LOCAL_DATETIME_SERIALIZER =
            new LocalDateTimeSerializer(DateTimeFormatter.ofPattern(DATETIME_FORMAT));

    private ObjectMapper objectMapper;
    private static Cipher cipher;

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            final JavaTimeModule module = new JavaTimeModule();
            module.addSerializer(LOCAL_DATETIME_SERIALIZER);
            objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).registerModule(module);
        }
        return objectMapper;
    }

    @Override
    public byte[] serialize(String s, T event) {
        try {
            return encrypt(getObjectMapper().writeValueAsString(event).getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            KafkaLogger.errorMessage(this.getClass(), e);
        }
        return new byte[0];
    }

    private static Cipher getCipher() throws NoSuchAlgorithmException, InvalidAlgorithmParameterException, NoSuchPaddingException, InvalidKeyException {
        if (cipher == null) {
            final CipherInitializer cipherInitializer = new CipherInitializer();
            cipher = cipherInitializer.prepareAndInitCipher(Cipher.ENCRYPT_MODE, eventEncryptionKey);
        }
        return cipher;
    }

    public byte[] encrypt(byte[] data) {
        if (eventEncryptionKey != null && !eventEncryptionKey.isEmpty() && data != null) {
            try {
                KafkaLogger.debug(this.getClass(), "Event encrypted!");
                return getCipher().doFinal(data);
            } catch (NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException | BadPaddingException | NoSuchPaddingException
                    | IllegalBlockSizeException e) {
                throw new RuntimeException(e);
            }
        }
        return data;
    }
}
