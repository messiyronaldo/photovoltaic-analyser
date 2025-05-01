package org.messiyronaldo.energy.model;

import com.google.gson.annotations.SerializedName;

import java.time.Instant;

public class EnergyPrice {
	@SerializedName("ts")
	private final Instant timestamp;
	private final Instant priceTimestamp;
	private final double pricePVPC;
	private final double priceSpot;

	@SerializedName("ss")
	private final String sourceSystem;

	public EnergyPrice(Instant timestamp, Instant priceTimestamp, double pricePVPC, double priceSpot, String sourceSystem) {
		this.timestamp = timestamp;
		this.priceTimestamp = priceTimestamp;
		this.pricePVPC = pricePVPC;
		this.priceSpot = priceSpot;
		this.sourceSystem = sourceSystem;
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

	public String getSourceSystem() {
		return sourceSystem;
	}

	@Override
	public String toString() {
		return "EnergyPrice{" +
				"timestamp=" + timestamp +
				", priceTimestamp=" + priceTimestamp +
				", pricePVPC=" + pricePVPC +
				", priceSpot=" + priceSpot +
				", sourceSystem='" + sourceSystem + '\'' +
				'}';
	}
}