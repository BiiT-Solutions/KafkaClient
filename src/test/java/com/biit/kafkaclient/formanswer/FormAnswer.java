package com.biit.kafkaclient.formanswer;

import java.util.Objects;

public class FormAnswer extends ReportFormAnswer {
	private Object answer;

	public FormAnswer() {
		super();
	}

	public void setAnswer(Object answer) {
		this.answer = answer;
	}

	public Object getAnswer() {
		return answer;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		FormAnswer that = (FormAnswer) o;
		return Objects.equals(answer, that.answer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), answer);
	}
}
