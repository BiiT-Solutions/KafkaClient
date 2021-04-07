package com.biit.kafkaclient.response;

import com.biit.kafkaclient.valuesofinterest.BasicEvent;

public abstract class EventResponse extends BasicEvent {

	private String group;
	private String parameter;

	public EventResponse() {
		super();
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getParameter() {
		return parameter;
	}

	public void setParameter(String parameter) {
		this.parameter = parameter;
	}

}
