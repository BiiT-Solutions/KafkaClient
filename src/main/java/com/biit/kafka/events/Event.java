package com.biit.kafka.events;


import com.biit.database.encryption.LocalDateTimeCryptoConverter;
import com.biit.database.encryption.StringCryptoConverter;
import com.biit.database.encryption.StringMapCryptoConverter;
import com.biit.database.encryption.UUIDCryptoConverter;
import com.biit.kafka.config.ObjectMapperFactory;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
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
    private static final int HASH_SEED = 31;

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
    private LocalDateTime createdAt;

    @Convert(converter = StringCryptoConverter.class)
    private String createdBy;

    @Convert(converter = StringCryptoConverter.class)
    private String entityType;

    @Convert(converter = StringCryptoConverter.class)
    private Object payload;

    @Convert(converter = StringMapCryptoConverter.class)
    private Map<String, String> customProperties;

    public Event() {
        super();
        customProperties = new HashMap<>();
    }

    public Event(EventPayload entity) {
        this(entity, entity.getClass().getName());
    }

    public Event(EventPayload entity, String entityType) {
        this();
        setEntity(entity);
        setEntityType(entityType);
        id = Uuid.randomUuid().toString();
        createdAt = LocalDateTime.now();
    }

    @JsonIgnore
    public void setEntity(EventPayload entity) {
        setPayload(entity);
    }


    @JsonIgnore
    public <T> T getEntity(Class<T> entityClass) {
        if (getPayload() != null) {
            return ObjectMapperFactory.getObjectMapper().convertValue(getPayload(), entityClass);
        }
        return null;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
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

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
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

    public void setCustomProperty(String key, String value) {
        if (customProperties == null) {
            customProperties = new HashMap<>();
        }
        customProperties.put(key, value);
    }

    public void setCustomProperty(EventCustomProperties key, String value) {
        if (key == null) {
            return;
        }
        setCustomProperty(key.getTag(), value);
    }

    public void setCustomProperties(Map<String, String> customProperties) {
        this.customProperties = customProperties;
    }

    public String getCustomProperty(EventCustomProperties property) {
        if (customProperties == null) {
            return null;
        }
        return customProperties.get(property.getTag());
    }

    @Override
    public String toString() {
        try {
            return "Event{"
                    + "payload='" + ObjectMapperFactory.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(payload) + '\''
                    + '}';
        } catch (JsonProcessingException e) {
            return "Event{"
                    + "payload='" + payload + '\''
                    + '}';
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Event event)) {
            return false;
        }

        if (getId() != null ? !getId().equals(event.getId()) : event.getId() != null) {
            return false;
        }
        if (getTo() != null ? !getTo().equals(event.getTo()) : event.getTo() != null) {
            return false;
        }
        if (getReplyTo() != null ? !getReplyTo().equals(event.getReplyTo()) : event.getReplyTo() != null) {
            return false;
        }
        if (getReplyToSessionId() != null ? !getReplyToSessionId().equals(event.getReplyToSessionId()) : event.getReplyToSessionId() != null) {
            return false;
        }
        if (getSessionId() != null ? !getSessionId().equals(event.getSessionId()) : event.getSessionId() != null) {
            return false;
        }
        if (getMessageId() != null ? !getMessageId().equals(event.getMessageId()) : event.getMessageId() != null) {
            return false;
        }
        if (getCorrelationId() != null ? !getCorrelationId().equals(event.getCorrelationId()) : event.getCorrelationId() != null) {
            return false;
        }
        if (getSubject() != null ? !getSubject().equals(event.getSubject()) : event.getSubject() != null) {
            return false;
        }
        if (getTenant() != null ? !getTenant().equals(event.getTenant()) : event.getTenant() != null) {
            return false;
        }
        if (getContentType() != null ? !getContentType().equals(event.getContentType()) : event.getContentType() != null) {
            return false;
        }
        if (getAuthorization() != null ? !getAuthorization().equals(event.getAuthorization()) : event.getAuthorization() != null) {
            return false;
        }
        if (getCreatedAt() != null ? !getCreatedAt().equals(event.getCreatedAt()) : event.getCreatedAt() != null) {
            return false;
        }
        if (getCreatedBy() != null ? !getCreatedBy().equals(event.getCreatedBy()) : event.getCreatedBy() != null) {
            return false;
        }
        if (getEntityType() != null ? !getEntityType().equals(event.getEntityType()) : event.getEntityType() != null) {
            return false;
        }
        if (getPayload() != null ? !getPayload().equals(event.getPayload()) : event.getPayload() != null) {
            return false;
        }
        return getCustomProperties() != null ? getCustomProperties().equals(event.getCustomProperties()) : event.getCustomProperties() == null;
    }

    @Override
    public int hashCode() {
        int result = getId() != null ? getId().hashCode() : 0;
        result = HASH_SEED * result + (getTo() != null ? getTo().hashCode() : 0);
        result = HASH_SEED * result + (getReplyTo() != null ? getReplyTo().hashCode() : 0);
        result = HASH_SEED * result + (getReplyToSessionId() != null ? getReplyToSessionId().hashCode() : 0);
        result = HASH_SEED * result + (getSessionId() != null ? getSessionId().hashCode() : 0);
        result = HASH_SEED * result + (getMessageId() != null ? getMessageId().hashCode() : 0);
        result = HASH_SEED * result + (getCorrelationId() != null ? getCorrelationId().hashCode() : 0);
        result = HASH_SEED * result + (getSubject() != null ? getSubject().hashCode() : 0);
        result = HASH_SEED * result + (getTenant() != null ? getTenant().hashCode() : 0);
        result = HASH_SEED * result + (getContentType() != null ? getContentType().hashCode() : 0);
        result = HASH_SEED * result + (getAuthorization() != null ? getAuthorization().hashCode() : 0);
        result = HASH_SEED * result + (getCreatedAt() != null ? getCreatedAt().hashCode() : 0);
        result = HASH_SEED * result + (getCreatedBy() != null ? getCreatedBy().hashCode() : 0);
        result = HASH_SEED * result + (getEntityType() != null ? getEntityType().hashCode() : 0);
        result = HASH_SEED * result + (getPayload() != null ? getPayload().hashCode() : 0);
        result = HASH_SEED * result + (getCustomProperties() != null ? getCustomProperties().hashCode() : 0);
        return result;
    }
}
