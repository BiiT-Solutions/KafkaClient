package com.biit.kafkaclient.response;


public class Response extends EventResponse {

	private Object value;

	public Response() {
		super();
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

}
