package org.messiyronaldo.energy.model;

import java.time.Instant;

public class EnergyPrice {
	private final Instant timestamp;
	private final Instant priceTimestamp;
	private final double pricePVPC;
	private final double priceSpot;

	public EnergyPrice(Instant timestamp, Instant priceTimestamp, double pricePVPC, double priceSpot) {
		this.timestamp = timestamp;
		this.priceTimestamp = priceTimestamp;
		this.pricePVPC = pricePVPC;
		this.priceSpot = priceSpot;
	}

	public Instant getTimestamp() {
		return timestamp;
	}

	public Instant getPriceTimestamp() {
		return priceTimestamp;
	}

	public double getPricePVPC() {
		return pricePVPC;
	}

	public double getPriceSpot() {
		return priceSpot;
	}

	@Override
	public String toString() {
		return "EnergyPrice{" +
				"timestamp=" + timestamp +
				", priceTimestamp=" + priceTimestamp +
				", pricePVPC=" + pricePVPC +
				", priceSpot=" + priceSpot +
				'}';
	}
}