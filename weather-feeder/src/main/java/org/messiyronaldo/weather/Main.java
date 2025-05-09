package org.messiyronaldo.weather;

import org.messiyronaldo.weather.control.*;
import org.messiyronaldo.weather.model.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Main {
	private static final long UPDATE_INTERVAL_MINUTES = 60 * 6;
	private static final int CONTROLLER_START_DELAY_SECONDS = 3;
	private static final List<WeatherController> controllers = new java.util.ArrayList<>();
	private static final Logger logger = LoggerFactory.getLogger(Main.class);

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
			weatherPublisher.start();
			logger.info("Weather publisher started successfully");
		} else {
			logger.error("Invalid store type. Use 'sql' or 'activemq'.");
			System.exit(1);
		}

		startWeatherControllers(monitoredLocations, weatherProvider, weatherStore, weatherPublisher);
		registerShutdownHook();
		keepApplicationRunning();
	}

	private static void validateArguments(String[] args) {
		if (args.length != 3) {
			logger.error("Invalid arguments. Usage: java -jar weather-feeder.jar <api-key> <database-file> <store-type>");
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
		logger.info("Starting weather monitoring for {} locations", locations.size());

		for (Location location : locations) {
			WeatherController controller = new WeatherController(location, provider, store, publisher, UPDATE_INTERVAL_MINUTES);
			controller.start();
			controllers.add(controller);
			logger.info("Controller started for: {}", location.getName());
			delayBetweenControllers();
		}

		logger.info("Application running. Data will update every {} minutes", UPDATE_INTERVAL_MINUTES);
	}

	private static void delayBetweenControllers() {
		try {
			TimeUnit.SECONDS.sleep(CONTROLLER_START_DELAY_SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.warn("Controller start delay interrupted");
		}
	}

	private static void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutting down weather-feeder application...");
			shutdownAllControllers();
		}));
	}

	private static void shutdownAllControllers() {
		for (WeatherController controller : controllers) {
			controller.shutdown();
		}
		logger.info("All weather controllers shut down successfully");
	}

	private static void keepApplicationRunning() {
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			logger.info("Application terminated");
		}
	}
}