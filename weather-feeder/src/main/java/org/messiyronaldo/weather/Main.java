package org.messiyronaldo.weather;

import org.messiyronaldo.weather.control.*;
import org.messiyronaldo.weather.model.Location;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
	private static final long UPDATE_INTERVAL_MINUTES = 60 * 6;
	private static final int CONTROLLER_START_DELAY_SECONDS = 3;
	private static final List<WeatherController> controllers = new java.util.ArrayList<>();

	public static void main(String[] args) {
		validateArguments(args);

		String apiKey = args[0];
		String databaseFileName = args[1];
		String storeType = args[2].toLowerCase();

		List<Location> monitoredLocations = createLocationsList();

		WeatherProvider weatherProvider = new OpenWeatherProvider(apiKey);
		WeatherStore weatherStore = null;
		WeatherPublisher weatherPublisher = null;

		if (storeType.equals("sql")) {
			weatherStore = new SQLiteWeatherStore(databaseFileName);
		} else if (storeType.equals("activemq")) {
			weatherPublisher = new ActiveMQWeatherPublisher();
		} else {
			System.err.println("Invalid store type. Use 'sql' or 'activemq'.");
			System.exit(1);
		}

		startWeatherControllers(monitoredLocations, weatherProvider, weatherStore, weatherPublisher);
		registerShutdownHook();
		keepApplicationRunning();
	}

	private static void validateArguments(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: java -jar weather-feeder.jar <api-key> <database-file> <store-type>");
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
												WeatherStore store,
												WeatherPublisher publisher) {
		System.out.println("Starting weather monitoring for " + locations.size() + " locations");

		for (Location location : locations) {
			WeatherController controller;
			if (store != null) {
				controller = new WeatherController(location, provider, store, null, UPDATE_INTERVAL_MINUTES);
			} else {
				controller = new WeatherController(location, provider, null, publisher, UPDATE_INTERVAL_MINUTES);
			}
			controllers.add(controller);
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
			shutdownAllControllers();
		}));
	}

	private static void shutdownAllControllers() {
		for (WeatherController controller : controllers) {
			controller.shutdown();
		}
		System.out.println("All weather controllers shut down successfully");
	}

	private static void keepApplicationRunning() {
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			System.out.println("Application terminated.");
		}
	}
}