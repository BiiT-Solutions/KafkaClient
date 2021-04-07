package com.biit.kafkaclient.valuesofinterest;

import java.util.Date;
import java.util.Objects;

public class Negotiation extends Opportunity {

	private int salesmanId;
	private String salesmanName;
	private int customerId;
	private String customerName;

	public Negotiation(String id, Date creationTime) {
		super(id, creationTime);
	}

	public Negotiation() {
		super();
	}

	public int getSalesmanId() {
		return salesmanId;
	}

	public void setSalesmanId(int salesmanId) {
		this.salesmanId = salesmanId;
	}

	public String getSalesmanName() {
		return salesmanName;
	}

	public void setSalesmanName(String salesmanName) {
		this.salesmanName = salesmanName;
	}

	public int getCustomerId() {
		return customerId;
	}

	public void setCustomerId(int custumerId) {
		this.customerId = custumerId;
	}

	public String getCustomerName() {
		return customerName;
	}

	public void setCustumerName(String custumerName) {
		this.customerName = custumerName;
	}

	@Override
	public String toString() {
		return getId() + " (" + getCreationTime() + ") : country='" + getCountry() + "', productId='" + getProductId() +
				"', salesmanId='" + getSalesmanId() + "', salesmanName='" + getSalesmanName() +
				"', customerId='" + getSalesmanId() + "', customerName='" + getCustomerName() + "'";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		Negotiation that = (Negotiation) o;
		return getSalesmanId() == that.getSalesmanId() &&
				getCustomerId() == that.getCustomerId() &&
				Objects.equals(getSalesmanName(), that.getSalesmanName()) &&
				Objects.equals(getCustomerName(), that.getCustomerName());
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), getSalesmanId(), getSalesmanName(), getCustomerId(), getCustomerName());
	}
}
