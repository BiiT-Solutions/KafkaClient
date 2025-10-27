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

    public static synchronized String getEncryptionKey() {
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

    public static synchronized String getPublicKey() {
        return publicKey;
    }

    private static synchronized void setPublicKey(String publicKey) {
        KafkaKeyProperty.publicKey = publicKey;
    }

    public static synchronized String getPrivateKey() {
        return privateKey;
    }

    private static synchronized void setPrivateKey(String privateKey) {
        KafkaKeyProperty.privateKey = privateKey;
    }

    public static synchronized String getNonce() {
        return nonce;
    }

    public static synchronized String getSalt() {
        return salt;
    }
}
