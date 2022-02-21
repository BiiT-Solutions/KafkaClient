package com.biit.kafkaclient.valuesofinterest;

import java.time.LocalDateTime;
import java.util.Objects;

public class Proposal extends Negotiation {
	private double amount;
	private long quantity;

	public Proposal(String id, LocalDateTime creationTime) {
		super(id, creationTime);
	}

	public Proposal() {
		super();
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	public long getQuantity() {
		return quantity;
	}

	public void setQuantity(long quantity) {
		this.quantity = quantity;
	}

	@Override
	public String toString() {
		return getEventId() + " (" + getCreationTime() + ") : country='" + getCountry() + "', productId='" + getProductId() +
				"', salesmanId='" + getSalesmanId() + "', salesmanName='" + getSalesmanName() +
				"', customerId='" + getSalesmanId() + "', customerName='" + getCustomerName() +
				"', amount='" + getAmount() + "', quantity='" + getQuantity() + "'";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		Proposal proposal = (Proposal) o;
		return Double.compare(proposal.getAmount(), getAmount()) == 0 &&
				getQuantity() == proposal.getQuantity();
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), getAmount(), getQuantity());
	}
}
