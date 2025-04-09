package org.messiyronaldo.weather;

import org.messiyronaldo.weather.control.*;
import org.messiyronaldo.weather.model.Location;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Uso: java -jar weather-feeder.jar <api-key>");
			System.exit(1);
		}

		List<Location> locations = Arrays.asList(
				new Location("Madrid", 40.4165, -3.7026),
				new Location("Las Palmas", 28.151286, -15.427340)
		);

		WeatherProvider weatherProvider = new OpenWeatherProvider(args[0]);
		WeatherStore weatherStore = new SQLiteWeatherStore("photovoltaic-data.db");

		final long updateIntervalMinutes = 60 * 6;

		System.out.println("Iniciando monitoreo de datos meteorológicos para " + locations.size() + " ubicaciones");

		for (Location location : locations) {
			new WeatherController(location, weatherProvider, weatherStore, updateIntervalMinutes);
			System.out.println("Controlador iniciado para: " + location.getName());

			try {
				TimeUnit.SECONDS.sleep(3);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		System.out.println("Aplicación en ejecución. Los datos se actualizarán cada " + updateIntervalMinutes + " minutos.");

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Cerrando aplicación weather-feeder...");
		}));

		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			System.out.println("Aplicación finalizada.");
		}
	}
}