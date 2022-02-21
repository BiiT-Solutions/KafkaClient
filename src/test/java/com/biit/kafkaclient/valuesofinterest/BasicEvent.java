package com.biit.kafkaclient.valuesofinterest;

import com.biit.eventstructure.event.IKafkaStorable;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public abstract class BasicEvent implements IKafkaStorable {
	private String id;
	private LocalDateTime creationTime;

	public BasicEvent(String id, LocalDateTime creationTime) {
		this.id = id;
		this.creationTime = creationTime;
	}

	public BasicEvent() {
		this.id = UUID.randomUUID().toString();
		this.creationTime = LocalDateTime.now();
	}

	public LocalDateTime getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(LocalDateTime creationTime) {
		this.creationTime = creationTime;
	}

	@Override
	public String toString() {
		return getEventId() + " (" + getCreationTime() + ")";
	}

	@Override
	public String getEventId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		BasicEvent event = (BasicEvent) o;
		return Objects.equals(getEventId(), event.getEventId()) &&
				Objects.equals(getCreationTime(), event.getCreationTime());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getEventId(), getCreationTime());
	}
}
