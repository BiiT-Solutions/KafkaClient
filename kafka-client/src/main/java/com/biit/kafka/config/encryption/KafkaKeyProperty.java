package com.biit.kafka.config.encryption;

import com.biit.database.encryption.logger.EncryptorLogger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class KafkaKeyProperty {

    private static String encryptionKey;
    private static String publicKey;
    private static String privateKey;
    private static String nonce;
    private static String salt;

    protected KafkaKeyProperty(@Value("${kafka.encryption.key:#{null}}") String encryptionKey,
                            @Value("${kafka.encryption.salt:#{null}}") String salt,
                            @Value("${kafka.public.key:#{null}}") String publicKey,
                            @Value("${kafka.private.key:#{null}}") String privateKey,
                            @Value("${kafka.nonce:#{null}}") String nonce,
                            @Value("${kafka.encryption.salt:#{null}}") String encryptionSalt,
                            @Value("${kafka.encryption.nonce:#{null}}") String encryptionNonce) {
        setEncryptionKey(encryptionKey);
        setPublicKey(publicKey);
        setPrivateKey(privateKey);
        setNonce(nonce);
        setSalt(salt);
        if (encryptionSalt != null && salt == null) {
            setEncryptionKey(encryptionSalt);
        }
        if (encryptionNonce != null && nonce == null) {
            setNonce(encryptionNonce);
        }
    }

    public static String getEncryptionKey() {
        return encryptionKey;
    }

    private static synchronized void setEncryptionKey(String encryptionKey) {
        EncryptorLogger.debug(KafkaKeyProperty.class.getName(), "Using '{}' as key", encryptionKey);
        KafkaKeyProperty.encryptionKey = encryptionKey;
    }

    private static synchronized void setNonce(String nonce) {
        EncryptorLogger.debug(KafkaKeyProperty.class.getName(), "Using '{}' as nonce", nonce);
        KafkaKeyProperty.nonce = nonce;
    }

    private static synchronized void setSalt(String salt) {
        EncryptorLogger.debug(KafkaKeyProperty.class.getName(), "Using '{}' as salt", salt);
        KafkaKeyProperty.salt = salt;
    }

    public static String getPublicKey() {
        return publicKey;
    }

    private static synchronized void setPublicKey(String publicKey) {
        KafkaKeyProperty.publicKey = publicKey;
    }

    public static String getPrivateKey() {
        return privateKey;
    }

    private static synchronized void setPrivateKey(String privateKey) {
        KafkaKeyProperty.privateKey = privateKey;
    }

    public static String getNonce() {
        return nonce;
    }

    public static String getSalt() {
        return salt;
    }
}
