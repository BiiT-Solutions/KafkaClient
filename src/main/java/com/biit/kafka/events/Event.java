package com.biit.kafka.events;


import com.biit.database.encryption.LocalDateTimeCryptoConverter;
import com.biit.database.encryption.StringCryptoConverter;
import com.biit.database.encryption.UUIDCryptoConverter;
import com.biit.kafka.exceptions.InvalidEventException;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import jakarta.persistence.Convert;
import org.apache.kafka.common.Uuid;

import java.time.LocalDateTime;
import java.util.UUID;

public abstract class Event<ENTITY> {

    @Convert(converter = StringCryptoConverter.class)
    private String id;

    @Convert(converter = StringCryptoConverter.class)
    private String to;

    @Convert(converter = StringCryptoConverter.class)
    private String replying;

    @Convert(converter = StringCryptoConverter.class)
    private String replyTo;

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

    @JsonDeserialize(using = LocalDateTimeDeserializer.class)
    @JsonSerialize(using = LocalDateTimeSerializer.class)
    @Convert(converter = LocalDateTimeCryptoConverter.class)
    private LocalDateTime createAt;

    @Convert(converter = StringCryptoConverter.class)
    private String createdBy;

    private transient ENTITY entity;

    @Convert(converter = StringCryptoConverter.class)
    private String payload;

    public Event() {
        super();
    }

    public Event(ENTITY entity) {
        this();
        setEntity(entity);
        id = Uuid.randomUuid().toString();
        createAt = LocalDateTime.now();
    }

    protected abstract TypeReference<ENTITY> getJsonParser();

    @JsonIgnore
    public void setEntity(ENTITY entity) {
        try {
            setPayload(new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).writeValueAsString(entity));
            this.entity = entity;
        } catch (JsonProcessingException e) {
            throw new InvalidEventException(this.getClass(), e);
        }
    }

    @JsonIgnore
    public ENTITY getEntity() {
        if (getPayload() != null && !getPayload().isEmpty()) {
            try {
                entity = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL).readValue(getPayload(), getJsonParser());
            } catch (JsonProcessingException e) {
                throw new InvalidEventException(this.getClass(), e);
            }
        }
        return entity;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
        this.entity = null;
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

    public String getReplying() {
        return replying;
    }

    public void setReplying(String replying) {
        this.replying = replying;
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

    @Override
    public String toString() {
        return "Event{"
                + "payload='" + payload + '\''
                + '}';
    }
}
