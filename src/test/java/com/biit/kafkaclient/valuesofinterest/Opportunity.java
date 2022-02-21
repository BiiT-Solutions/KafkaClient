package com.biit.kafkaclient.valuesofinterest;

import java.time.LocalDateTime;
import java.util.Date;

public class Opportunity extends Lead {

	public Opportunity(String id, LocalDateTime creationTime) {
		super(id, creationTime);
	}

	public Opportunity() {
		super();
	}
}
