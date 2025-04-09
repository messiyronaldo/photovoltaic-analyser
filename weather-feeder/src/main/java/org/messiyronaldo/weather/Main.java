package org.messiyronaldo.weather;

import org.messiyronaldo.weather.control.*;
import org.messiyronaldo.weather.model.Location;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
	private static final long UPDATE_INTERVAL_MINUTES = 60 * 6;
	private static final String DATABASE_FILENAME = "photovoltaic-data.db";
	private static final int CONTROLLER_START_DELAY_SECONDS = 3;

	public static void main(String[] args) {
		validateArguments(args);

		String apiKey = args[0];
		List<Location> monitoredLocations = createLocationsList();

		WeatherProvider weatherProvider = new OpenWeatherProvider(apiKey);
		WeatherStore weatherStore = new SQLiteWeatherStore(DATABASE_FILENAME);

		startWeatherControllers(monitoredLocations, weatherProvider, weatherStore);
		registerShutdownHook();
		keepApplicationRunning();
	}

	private static void validateArguments(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: java -jar weather-feeder.jar <api-key>");
			System.exit(1);
		}
	}

	private static List<Location> createLocationsList() {
		return Arrays.asList(
				new Location("Madrid", 40.4165, -3.7026),
				new Location("Las Palmas", 28.151286, -15.427340)
		);
	}

	private static void startWeatherControllers(List<Location> locations,
												WeatherProvider provider,
												WeatherStore store) {
		System.out.println("Starting weather monitoring for " + locations.size() + " locations");

		for (Location location : locations) {
			new WeatherController(location, provider, store, UPDATE_INTERVAL_MINUTES);
			System.out.println("Controller started for: " + location.getName());
			delayBetweenControllers();
		}

		System.out.println("Application running. Data will update every " +
				UPDATE_INTERVAL_MINUTES + " minutes.");
	}

	private static void delayBetweenControllers() {
		try {
			TimeUnit.SECONDS.sleep(CONTROLLER_START_DELAY_SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private static void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Shutting down weather-feeder application...");
		}));
	}

	private static void keepApplicationRunning() {
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			System.out.println("Application terminated.");
		}
	}
}