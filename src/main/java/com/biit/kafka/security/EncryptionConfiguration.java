package com.biit.kafka.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class EncryptionConfiguration {

    public static String eventEncryptionKey;

    public EncryptionConfiguration(@Value("${kafka.encryption.key}") String eventEncryptionKey) {
        setEventEncryptionKey(eventEncryptionKey);
    }

    private static synchronized  void setEventEncryptionKey(String eventEncryptionKey) {
        EncryptionConfiguration.eventEncryptionKey = eventEncryptionKey;
    }


}
