package com.biit.kafkaclient.valuesofinterest;

import java.util.Date;

public class Opportunity extends Lead {

	public Opportunity(String id, Date creationTime) {
		super(id, creationTime);
	}

	public Opportunity() {
		super();
	}
}
