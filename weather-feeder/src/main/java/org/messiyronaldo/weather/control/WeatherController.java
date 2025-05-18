package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class WeatherController {
	private static final Logger logger = LoggerFactory.getLogger(WeatherController.class);
	private final Location location;
	private final WeatherProvider provider;
	private final WeatherStore store;
	private final WeatherPublisher publisher;
	private final Timer timer;
	private final long updateIntervalMinutes;

	public WeatherController(Location location,
						   WeatherProvider provider,
						   WeatherStore store,
						   WeatherPublisher publisher,
						   long updateIntervalMinutes) {
		this.location = location;
		this.provider = provider;
		this.store = store;
		this.publisher = publisher;
		this.updateIntervalMinutes = updateIntervalMinutes;
		this.timer = new Timer("WeatherUpdate-" + location.getName(), true);
		logger.info("Weather controller initialized for location: {}", location.getName());
	}

	public void start() {
		long intervalMillis = updateIntervalMinutes * 60 * 1000;
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				updateWeatherData();
			}
		}, 0, intervalMillis);
	}

	private void updateWeatherData() {
		try {
			List<Weather> forecasts = provider.getWeatherForecasts(location);
			logger.info("Retrieved {} weather forecasts for location: {}",
				forecasts.size(), location.getName());

			if (store != null) {
				store.saveWeatherForecasts(forecasts);
				logger.info("Weather forecasts saved to store for location: {}", location.getName());
			}

			if (publisher != null) {
				for (Weather forecast : forecasts) {
					publisher.publish(forecast);
				}
				logger.info("Weather forecasts published for location: {}", location.getName());
			}
		} catch (Exception e) {
			logger.error("Error updating weather data for location {}: {}",
				location.getName(), e.getMessage(), e);
		}
	}

	public void shutdown() {
		if (timer != null) {
			timer.cancel();
			logger.info("Weather controller stopped for location: {}", location.getName());
		}

		if (publisher != null) {
			try {
				publisher.close();
				logger.info("Weather publisher closed successfully for location: {}", location.getName());
			} catch (Exception e) {
				logger.error("Error closing weather publisher for location {}: {}",
					location.getName(), e.getMessage(), e);
			}
		}
	}
}