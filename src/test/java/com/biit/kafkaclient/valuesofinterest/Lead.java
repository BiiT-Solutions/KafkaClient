package com.biit.kafkaclient.valuesofinterest;

import java.util.Date;
import java.util.Objects;

public class Lead extends BasicEvent {

	private long productId;
	private String country;

	public Lead(String id, Date creationTime) {
		super(id, creationTime);
	}

	public Lead() {
		super();
	}

	public long getProductId() {
		return productId;
	}

	public void setProductId(long productId) {
		this.productId = productId;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	@Override
	public String toString() {
		return getId() + " (" + getCreationTime() + ") : country='" + getCountry() + "', productId='" + getProductId() + "'";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		Lead lead = (Lead) o;
		return getProductId() == lead.getProductId() &&
				Objects.equals(getCountry(), lead.getCountry());
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), getProductId(), getCountry());
	}
}
