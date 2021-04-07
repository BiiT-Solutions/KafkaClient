package com.biit.kafkaclient.valuesofinterest;

import com.biit.eventstructure.event.IKafkaStorable;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Date;
import java.util.Objects;
import java.util.UUID;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class")
public abstract class BasicEvent implements IKafkaStorable {
	private String id;
	private Date creationTime;

	public BasicEvent(String id, Date creationTime) {
		this.id = id;
		this.creationTime = creationTime;
	}

	public BasicEvent() {
		this.id = UUID.randomUUID().toString();
		this.creationTime = new Date();
	}

	public Date getCreationTime() {
		return creationTime;
	}

	public void setCreationTime(Date creationTime) {
		this.creationTime = creationTime;
	}

	@Override
	public String toString() {
		return getId() + " (" + getCreationTime() + ")";
	}

	public String getId() {
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
		return Objects.equals(getId(), event.getId()) &&
				Objects.equals(getCreationTime(), event.getCreationTime());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId(), getCreationTime());
	}
}
