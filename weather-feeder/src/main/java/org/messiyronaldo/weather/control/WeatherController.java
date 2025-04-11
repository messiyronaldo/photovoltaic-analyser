package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class WeatherController {
	private final Location location;
	private final WeatherProvider weatherProvider;
	private final WeatherStore weatherStore;
	private final WeatherPublisher weatherPublisher;
	private final Timer timer;

	public WeatherController(Location location, WeatherProvider weatherProvider, WeatherStore weatherStore, WeatherPublisher weatherPublisher, long updateIntervalMinutes) {
		this.location = location;
		this.weatherProvider = weatherProvider;
		this.weatherStore = weatherStore;

		this.weatherPublisher = weatherPublisher;

		if (this.weatherPublisher != null) {
			this.weatherPublisher.start();
		}

		this.timer = createAndScheduleTimer(updateIntervalMinutes);
	}

	private Timer createAndScheduleTimer(long updateIntervalMinutes) {
		Timer timer = new Timer("WeatherUpdate-" + location.getName(), false);
		long updateIntervalMillis = updateIntervalMinutes * 60 * 1000;

		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				updateWeatherData();
			}
		}, 0, updateIntervalMillis);
		return timer;
	}

	private void updateWeatherData() {
		try {
			List<Weather> weatherData = weatherProvider.getHourlyForecast(location);
			weatherStore.saveWeatherForecasts(weatherData);
			logSuccessfulUpdate();
		} catch (IOException e) {
			logUpdateError(e);
		}
	}

	private void logSuccessfulUpdate() {
		System.out.println("Weather data updated for: " + location.getName() +
				" at " + LocalDateTime.now());
	}

	private void logUpdateError(Exception e) {
		System.err.println("Error retrieving weather data: " + e.getMessage());
	}

	public void shutdown() {
		if (timer != null) {
			timer.cancel();
			System.out.println("Weather controller for " + location.getName() + " stopped");
		}
	}
}