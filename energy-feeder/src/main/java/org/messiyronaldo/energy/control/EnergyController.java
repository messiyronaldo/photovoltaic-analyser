package org.messiyronaldo.energy.control;

import org.messiyronaldo.energy.model.EnergyPrice;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
		this.timer = createAndScheduleTimer(updateIntervalMinutes);
	}

	private Timer createAndScheduleTimer(long updateIntervalMinutes) {
		Timer timer = new Timer("EnergyUpdateTimer", false);
		long intervalMillis = updateIntervalMinutes * 60 * 1000;

		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				updateEnergyPrices();
			}
		}, 0, intervalMillis);

		return timer;
	}

	private void updateEnergyPrices() {
		try {
			LocalDate today = LocalDate.now();
			List<EnergyPrice> prices = energyProvider.getEnergyPrices(today);

			processRetrievedPrices(today, prices);
		} catch (IOException e) {
			logError(e);
		}
	}

	private void processRetrievedPrices(LocalDate date, List<EnergyPrice> prices) {
		if (prices != null && !prices.isEmpty()) {
			energyStore.saveEnergyPrices(prices);
			logSuccessfulUpdate(date, prices.size());
		} else {
			logEmptyUpdate(date);
		}
	}

	private void logSuccessfulUpdate(LocalDate date, int count) {
		System.out.println("Energy price data updated for: " + date +
				" at " + LocalDateTime.now() +
				" - " + count + " records");
	}

	private void logEmptyUpdate(LocalDate date) {
		System.out.println("No energy price data retrieved for: " + date);
	}

	private void logError(Exception e) {
		System.err.println("Error retrieving energy price data: " + e.getMessage());
	}

	public void shutdown() {
		if (timer != null) {
			timer.cancel();
			System.out.println("Energy controller stopped");
		}
	}
}