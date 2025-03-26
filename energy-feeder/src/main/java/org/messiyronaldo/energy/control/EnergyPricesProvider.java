package org.messiyronaldo.energy.control;

import org.messiyronaldo.energy.model.EnergyPrice;
import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

public interface EnergyPricesProvider {
	List<EnergyPrice> getEnergyPrices(LocalDate date) throws IOException;
}