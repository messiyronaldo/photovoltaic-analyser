package org.messiyronaldo;

import org.messiyronaldo.eventstore.control.EventStore;
import org.messiyronaldo.eventstore.control.EventStoreManager;
import org.messiyronaldo.eventstore.control.Subscriber;
import org.messiyronaldo.eventstore.control.SubscriberActiveMQ;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
	private static final Logger logger = LoggerFactory.getLogger(Main.class);
	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String CLIENT_BASE_ID = "MessiyRonaldo";
	private static final String WEATHER_TOPIC = "prediction.Weather";
	private static final String ENERGY_TOPIC = "prediction.Energy";

	private static Subscriber weatherSubscriber;
	private static Subscriber energySubscriber;

	public static void main(String[] args) {
		logger.info("Starting Event Store Builder...");

		EventStore eventStoreManager = new EventStoreManager();

		weatherSubscriber = new SubscriberActiveMQ(
				BROKER_URL,
				CLIENT_BASE_ID + "_Weather",
				CLIENT_BASE_ID + "_WeatherSub",
				eventStoreManager);

		energySubscriber = new SubscriberActiveMQ(
				BROKER_URL,
				CLIENT_BASE_ID + "_Energy",
				CLIENT_BASE_ID + "_EnergySub",
				eventStoreManager);

		weatherSubscriber.start();
		weatherSubscriber.subscribe(WEATHER_TOPIC);

		energySubscriber.start();
		energySubscriber.subscribe(ENERGY_TOPIC);

		registerShutdownHook();

		logger.info("Event Store Builder running and subscribed to topics");

		waitForever();
	}

	private static void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Shutting down Event Store Builder...");
			if (weatherSubscriber != null) weatherSubscriber.close();
			if (energySubscriber != null) energySubscriber.close();
		}));
	}

	private static void waitForever() {
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			logger.info("Application terminated.");
		}
	}
}