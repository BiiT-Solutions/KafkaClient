package com.biit.kafka.events;


import com.biit.database.encryption.LocalDateTimeCryptoConverter;
import com.biit.database.encryption.StringCryptoConverter;
import com.biit.database.encryption.StringMapCryptoConverter;
import com.biit.database.encryption.UUIDCryptoConverter;
import com.biit.kafka.config.ObjectMapperFactory;
import com.biit.kafka.exceptions.InvalidEventException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import jakarta.persistence.Convert;
import org.apache.kafka.common.Uuid;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Event {

    @Convert(converter = StringCryptoConverter.class)
    private String id;

    @Convert(converter = StringCryptoConverter.class)
    private String to;

    @Convert(converter = StringCryptoConverter.class)
    private String replyTo;

    @Convert(converter = StringCryptoConverter.class)
    private String replyToSessionId;

    @Convert(converter = UUIDCryptoConverter.class)
    private UUID sessionId;

    @Convert(converter = UUIDCryptoConverter.class)
    private UUID messageId;

    @Convert(converter = UUIDCryptoConverter.class)
    private UUID correlationId;

    @Convert(converter = StringCryptoConverter.class)
    private String subject;

    @Convert(converter = StringCryptoConverter.class)
    private String tenant;

    @Convert(converter = StringCryptoConverter.class)
    private String contentType;

    @Convert(converter = StringCryptoConverter.class)
    private String authorization;

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @Convert(converter = LocalDateTimeCryptoConverter.class)
    private LocalDateTime createAt;

    @Convert(converter = StringCryptoConverter.class)
    private String createdBy;

    @Convert(converter = StringCryptoConverter.class)
    private String entityType;

    @Convert(converter = StringCryptoConverter.class)
    private String payload;

    @Convert(converter = StringMapCryptoConverter.class)
    private Map<String, String> customProperties;

    public Event() {
        super();
        customProperties = new HashMap<>();
    }

    public Event(Object entity) {
        this();
        setEntity(entity);
        id = Uuid.randomUuid().toString();
        createAt = LocalDateTime.now();
    }

    @JsonIgnore
    public void setEntity(Object entity) {
        try {
            setPayload(ObjectMapperFactory.getObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).writeValueAsString(entity));
        } catch (JsonProcessingException e) {
            throw new InvalidEventException(this.getClass(), e);
        }
    }


    @JsonIgnore
    public <T> T getEntity(Class<T> entityClass) {
        if (getPayload() != null && !getPayload().isEmpty()) {
            try {
                return new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL)
                        .readValue(getPayload(), entityClass);
            } catch (JsonProcessingException e) {
                throw new InvalidEventException(this.getClass(), e);
            }
        }
        return null;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getReplyTo() {
        return replyTo;
    }

    public void setReplyTo(String replyTo) {
        this.replyTo = replyTo;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    public void setSessionId(UUID sessionId) {
        this.sessionId = sessionId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public void setMessageId(UUID messageId) {
        this.messageId = messageId;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(UUID correlationId) {
        this.correlationId = correlationId;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public LocalDateTime getCreateAt() {
        return createAt;
    }

    public void setCreateAt(LocalDateTime createAt) {
        this.createAt = createAt;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getReplyToSessionId() {
        return replyToSessionId;
    }

    public void setReplyToSessionId(String replyToSessionId) {
        this.replyToSessionId = replyToSessionId;
    }

    public String getAuthorization() {
        return authorization;
    }

    public void setAuthorization(String authorization) {
        this.authorization = authorization;
    }

    public Map<String, String> getCustomProperties() {
        return customProperties;
    }

    public void setCustomProperties(Map<String, String> customProperties) {
        this.customProperties = customProperties;
    }

    @Override
    public String toString() {
        return "Event{"
                + "payload='" + payload + '\''
                + '}';
    }
}
