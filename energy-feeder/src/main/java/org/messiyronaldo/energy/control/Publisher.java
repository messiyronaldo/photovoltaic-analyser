package org.messiyronaldo.energy.control;

import org.messiyronaldo.energy.model.EnergyPrice;

public interface Publisher {
	void start();
	void publish(EnergyPrice event);
	void close();
}
