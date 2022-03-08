package com.biit.kafka;

import com.biit.cipher.CipherInitializer;
import com.biit.kafka.logger.KafkaLogger;
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
import java.security.spec.InvalidKeySpecException;
import java.time.format.DateTimeFormatter;

import static com.biit.cipher.EncryptionConfiguration.encryptionKey;

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
    public byte[] serialize(String topic, T event) {
        try {
            String data = getObjectMapper().writeValueAsString(event);
            KafkaLogger.debug(this.getClass(), "For encrypt '{}'.", data);
            return encrypt(data.getBytes(StandardCharsets.UTF_8));
        } catch (JsonProcessingException e) {
            KafkaLogger.errorMessage(this.getClass(), e);
        }
        return new byte[0];
    }

    public byte[] encrypt(byte[] data) {
        if (encryptionKey != null && !encryptionKey.isEmpty() && data != null) {
            try {
                byte[] encryptedData = CipherInitializer.getCipherForEncrypt().doFinal(data);
                if (KafkaLogger.isDebugEnabled()) {
                    KafkaLogger.debug(this.getClass(), "Encrypted as '{}'.", byteArrayToHex(encryptedData));
                }
                return encryptedData;
            } catch (NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException | BadPaddingException |
                    NoSuchPaddingException | IllegalBlockSizeException | InvalidKeySpecException e) {
                CipherInitializer.resetCipherForEncrypt();
                throw new RuntimeException(e);
            }
        }
        return data;
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
