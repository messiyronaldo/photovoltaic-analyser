package org.messiyronaldo.energy.control;

import org.messiyronaldo.energy.model.EnergyPrice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import java.util.Timer;
import java.util.TimerTask;

public class EnergyController {
	private static final Logger logger = LoggerFactory.getLogger(EnergyController.class);
	private final EnergyPricesProvider energyProvider;
	private final EnergyPricesStore energyStore;
	private final EnergyPublisher energyPublisher;
	private final Timer timer;

	public EnergyController(EnergyPricesProvider energyProvider,
						  EnergyPricesStore energyStore,
						  EnergyPublisher energyPublisher,
						  long updateIntervalMinutes) {
		this.energyProvider = energyProvider;
		this.energyStore = energyStore;
		this.energyPublisher = energyPublisher;
		this.timer = createAndScheduleTimer(updateIntervalMinutes);
		logger.info("Energy controller initialized with update interval: {} minutes", updateIntervalMinutes);
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
		logger.debug("Timer scheduled with interval: {} minutes", updateIntervalMinutes);
		return timer;
	}

	private void updateEnergyPrices() {
		try {
			LocalDate today = LocalDate.now();
			List<EnergyPrice> prices = energyProvider.getEnergyPrices(today);

			processRetrievedPrices(today, prices);
		} catch (IOException e) {
			logUpdateError(e);
		}
	}

	private void processRetrievedPrices(LocalDate date, List<EnergyPrice> prices) {
		if (prices != null && !prices.isEmpty()) {
			if (energyStore != null) {
				energyStore.saveEnergyPrices(prices);
				logger.info("Energy prices saved to store for date: {}", date);
			}

			if (energyPublisher != null) {
				for (EnergyPrice price : prices) {
					energyPublisher.publish(price);
				}
				logger.info("Energy prices published for date: {}", date);
			}

			logSuccessfulUpdate(date, prices.size());
		} else {
			logEmptyUpdate(date);
		}
	}

	private void logSuccessfulUpdate(LocalDate date, int count) {
		logger.info("Energy price data updated for date: {} at {} - {} records processed",
			date, LocalDateTime.now(), count);
	}

	private void logEmptyUpdate(LocalDate date) {
		logger.warn("No energy price data retrieved for date: {}", date);
	}

	private void logUpdateError(Exception e) {
		logger.error("Failed to update energy price data: {}", e.getMessage(), e);
	}

	public void shutdown() {
		if (timer != null) {
			timer.cancel();
			logger.info("Energy controller stopped");
		}

		if (energyPublisher != null) {
			try {
				energyPublisher.close();
				logger.info("Energy publisher closed successfully");
			} catch (Exception e) {
				logger.error("Error closing energy publisher: {}", e.getMessage(), e);
			}
		}
	}
}