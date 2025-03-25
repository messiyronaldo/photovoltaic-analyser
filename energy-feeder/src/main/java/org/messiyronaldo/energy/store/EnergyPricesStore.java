package org.messiyronaldo.energy.store;

import org.messiyronaldo.energy.model.EnergyPrice;
import java.time.Instant;
import java.util.List;

public interface EnergyPricesStore {
	void saveEnergyPrices(List<EnergyPrice> prices);
	List<EnergyPrice> getEnergyPrices(Instant startTime, Instant endTime);
}