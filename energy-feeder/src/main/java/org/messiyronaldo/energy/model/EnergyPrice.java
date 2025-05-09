package org.messiyronaldo.energy.model;

import java.time.Instant;

public class EnergyPrice {
	private final Instant ts;
	private final Instant priceTimestamp;
	private final double pricePVPC;
	private final double priceSpot;
	private final String ss;

	public EnergyPrice(Instant ts, Instant priceTimestamp, double pricePVPC, double priceSpot, String ss) {
		this.ts = ts;
		this.priceTimestamp = priceTimestamp;
		this.pricePVPC = pricePVPC;
		this.priceSpot = priceSpot;
		this.ss = ss;
	}

	public Instant getTs() {
		return ts;
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

	public String getSs() {
		return ss;
	}

	@Override
	public String toString() {
		return "EnergyPrice{" +
				"timestamp=" + ts +
				", priceTimestamp=" + priceTimestamp +
				", pricePVPC=" + pricePVPC +
				", priceSpot=" + priceSpot +
				", sourceSystem='" + ss + '\'' +
				'}';
	}
}