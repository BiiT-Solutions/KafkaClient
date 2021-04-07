package com.biit.kafkaclient;

import com.biit.kafkaclient.formanswer.FormAnswer;
import com.biit.kafkaclient.response.Response;
import com.biit.kafkaclient.valuesofinterest.*;

import java.util.Date;
import java.util.Random;

public class EventGenerator {

	private final static Random RANDOM = new Random();

	public static BasicEvent generateResponse() {
		Response response = new Response();
		response.setId(new Random().nextLong() + "");
		response.setValue(generateNewBasicEvent());
		response.setCreationTime(new Date());
		response.setGroup("group");
		response.setParameter("parameter");
		return response;
	}

	public static BasicEvent generateNewBasicEvent() {
		BasicEvent event = new Lead();
		switch ((int) (Math.random() * 6)) {
			case 1:
				event = new Opportunity();
				break;
			case 2:
				event = new Negotiation();
				break;
			case 3:
				event = new Proposal();
				break;
			case 4:
				event = new Win();
				break;
			case 5:
				event = new FormAnswer();
				((FormAnswer) event).setAnswer(Math.random() + "");
				((FormAnswer) event).setQuestion(Math.random() + "?");
				return event;
		}
		populateBasicEvent(event);
		return event;
	}

	public static BasicEvent generateNewBasicEvent(long minTimestamp, long maxTimestamp) {
		BasicEvent result = generateNewBasicEvent();
		long timestamp = minTimestamp + (long) (Math.random() * ((double) (maxTimestamp - minTimestamp)));
		result.setCreationTime(new Date(timestamp));
		return result;
	}

	private static void populateBasicEvent(BasicEvent event) {
		event.setCreationTime(new Date(System.currentTimeMillis()));
		event.setId(RANDOM.nextLong() + "");
		if (event instanceof Lead) {
			populateLead((Lead) event);
		}
	}

	private static void populateLead(Lead lead) {
		lead.setCountry("Mock country");
		lead.setProductId(RANDOM.nextLong());
		if (lead instanceof Opportunity) {
			populateOpportunity((Opportunity) lead);
		}
	}

	private static void populateOpportunity(Opportunity opportunity) {
		if (opportunity instanceof Negotiation) {
			populateNegotiation((Negotiation) opportunity);
		}
	}

	private static void populateNegotiation(Negotiation negotiation) {
		negotiation.setCustomerId(RANDOM.nextInt());
		negotiation.setCustumerName("Jose");
		negotiation.setSalesmanId(RANDOM.nextInt());
		negotiation.setSalesmanName("Pepe");
		if (negotiation instanceof Proposal) {
			populateProposal((Proposal) negotiation);
		}
	}

	private static void populateProposal(Proposal proposal) {
		proposal.setAmount(RANDOM.nextDouble());
		proposal.setQuantity(RANDOM.nextInt());
//		if(proposal instanceof Win){
//			populateWin((Win) proposal);
//		}
	}

//	private void populateWin(Win win) {
//	}


}
