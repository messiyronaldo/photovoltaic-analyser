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
	private final EnergyPublisher publisher;
	private final Timer timer;

	public EnergyController(EnergyPricesProvider energyProvider,
							EnergyPricesStore energyStore,
							EnergyPublisher publisher,
							long updateIntervalMinutes) {
		this.energyProvider = energyProvider;
		this.energyStore = energyStore;
		this.publisher = publisher;
		this.timer = createAndScheduleTimer(updateIntervalMinutes);

		if (this.publisher != null) {
			this.publisher.start();
		}
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

			if (publisher != null) {
				prices.forEach(price -> {
					try {
						publisher.publish(price);
						System.out.println("Published energy price for: " + price.getPriceTimestamp());
					} catch (Exception e) {
						System.err.println("Failed to publish energy price: " + e.getMessage());
					}
				});
			}

			if (energyStore != null) {
				energyStore.saveEnergyPrices(prices);
			}

			logSuccessfulUpdate(today, prices.size());
		} catch (IOException e) {
			logError(e);
		}
	}

	private void logSuccessfulUpdate(LocalDate date, int count) {
		System.out.println("Energy price data updated for: " + date +
				" at " + LocalDateTime.now() +
				" - " + count + " records");
	}

	private void logError(Exception e) {
		System.err.println("Error retrieving energy price data: " + e.getMessage());
	}

	public void shutdown() {
		if (timer != null) {
			timer.cancel();
			System.out.println("Energy controller stopped");
		}
		if (publisher != null) {
			publisher.close();
		}
	}
}