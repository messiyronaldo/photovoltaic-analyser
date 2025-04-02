package org.messiyronaldo.weather;

import org.messiyronaldo.weather.control.*;
import org.messiyronaldo.weather.model.Location;

import java.util.Arrays;
import java.util.List;

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

		for (int i = 0; i < locations.size(); i++) {
			Location location = locations.get(i);

			// Crear el controlador
			WeatherController weatherController = new WeatherController(
					location, weatherProvider, weatherStore, updateIntervalMinutes);

			System.out.println("Controlador iniciado para: " + location.getName());

			// Pequeña pausa entre inicializaciones para evitar accesos simultáneos
			if (i < locations.size() - 1) {
				try {
					// Esperar 10 segundos entre inicializaciones
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}

		System.out.println("Aplicación en ejecución. Los datos se actualizarán cada " + updateIntervalMinutes + " minutos.");

		// Mantener la aplicación en ejecución
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			System.out.println("Aplicación finalizada.");
		}
	}
}