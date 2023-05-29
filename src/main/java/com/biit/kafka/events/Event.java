package com.biit.kafka.events;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Event<ENTITY> {
    private String id;
    private String to;
    private String replying;
    private String replyTo;
    private String sessionId;
    private String messageId;
    private String correlationId;
    private String subject;
    private String contentType;

    private transient ENTITY entity;
    private String payload;

    public Event(){
        super();
    }

    public Event(ENTITY entity) {
        this();
        setEntity(entity);
    }


    protected TypeReference<ENTITY> getJsonParser() {
        return new TypeReference<>() {
        };
    }

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

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }
}
