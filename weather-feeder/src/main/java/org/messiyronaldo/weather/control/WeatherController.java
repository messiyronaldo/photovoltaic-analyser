package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class WeatherController {
	private final Location location;
	private final WeatherProvider weatherProvider;
	private final WeatherStore weatherStore;
	private final Timer timer;

	public WeatherController(Location location, WeatherProvider weatherProvider, WeatherStore weatherStore, long updateIntervalMinutes) {
		this.location = location;
		this.weatherProvider = weatherProvider;
		this.weatherStore = weatherStore;

		this.timer = new Timer("WeatherUpdateTimer", false);
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				WeatherController.this.run();
			}
		}, 0, updateIntervalMinutes * 60 * 1000); // Convertir minutos a milisegundos
	}

	public void run() {
		try {
			List<Weather> weatherData = weatherProvider.getHourlyForecast(location);
			weatherStore.saveWeatherForecasts(weatherData);
			System.out.println("Datos meteorológicos actualizados para: " + location.getName() +
					" a las " + java.time.LocalDateTime.now());
		} catch (IOException e) {
			System.err.println("Error al obtener datos meteorológicos: " + e.getMessage());
		}
	}
}