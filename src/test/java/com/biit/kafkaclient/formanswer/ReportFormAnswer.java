package com.biit.kafkaclient.formanswer;


import com.biit.kafkaclient.valuesofinterest.BasicEvent;

import java.util.Objects;

public abstract class ReportFormAnswer extends BasicEvent {
	private String uuid;
	private int managerId;
	private int journeyId;
	private String question;

	public ReportFormAnswer() {
		super();
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public int getManagerId() {
		return managerId;
	}

	public void setManagerId(int idManager) {
		this.managerId = idManager;
	}

	public int getJourneyId() {
		return journeyId;
	}

	public void setJourneyId(int idJourney) {
		this.journeyId = idJourney;
	}

	public String getQuestion() {
		return question;
	}

	public void setQuestion(String question) {
		this.question = question;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		ReportFormAnswer that = (ReportFormAnswer) o;
		return managerId == that.managerId &&
				journeyId == that.journeyId &&
				Objects.equals(uuid, that.uuid) &&
				Objects.equals(question, that.question);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), uuid, managerId, journeyId, question);
	}
}
