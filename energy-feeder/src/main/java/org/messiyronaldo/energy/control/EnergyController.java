package org.messiyronaldo.energy.control;

import org.messiyronaldo.energy.model.EnergyPrice;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class EnergyController {
	private final EnergyPricesProvider energyProvider;
	private final EnergyPricesStore energyStore;
	private final Timer timer;

	public EnergyController(EnergyPricesProvider energyProvider, EnergyPricesStore energyStore, long updateIntervalMinutes) {
		this.energyProvider = energyProvider;
		this.energyStore = energyStore;

		this.timer = new Timer("EnergyUpdateTimer", false);
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				EnergyController.this.run();
			}
		}, 0, updateIntervalMinutes * 60 * 1000); // Convertir minutos a milisegundos
	}

	public void run() {
		try {
			// Obtener precios para el día actual
			LocalDate today = LocalDate.now();
			List<EnergyPrice> energyPrices = energyProvider.getEnergyPrices(today);

			if (energyPrices != null && !energyPrices.isEmpty()) {
				energyStore.saveEnergyPrices(energyPrices);
				System.out.println("Datos de precios de energía actualizados para: " + today +
						" a las " + java.time.LocalDateTime.now() +
						" - " + energyPrices.size() + " registros");
			} else {
				System.out.println("No se obtuvieron datos de precios de energía para: " + today);
			}
		} catch (IOException e) {
			System.err.println("Error al obtener datos de precios de energía: " + e.getMessage());
		}
	}
}