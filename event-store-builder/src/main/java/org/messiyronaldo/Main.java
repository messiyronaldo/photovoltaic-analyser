package org.messiyronaldo;

import org.messiyronaldo.eventstore.control.EventStore;
import org.messiyronaldo.eventstore.control.EventStoreManager;
import org.messiyronaldo.eventstore.control.Subscriber;
import org.messiyronaldo.eventstore.control.SubscriberActiveMQ;

public class Main {
	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String CLIENT_BASE_ID = "MessiyRonaldo";
	private static final String WEATHER_TOPIC = "prediction.Weather";
	private static final String ENERGY_TOPIC = "prediction.Energy";

	private static Subscriber weatherSubscriber;
	private static Subscriber energySubscriber;

	public static void main(String[] args) {
		System.out.println("Starting Event Store Builder...");

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

		System.out.println("Event Store Builder running and subscribed to topics");

		waitForever();
	}

	private static void registerShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Shutting down Event Store Builder...");
			if (weatherSubscriber != null) weatherSubscriber.close();
			if (energySubscriber != null) energySubscriber.close();
		}));
	}

	private static void waitForever() {
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			System.out.println("Application terminated.");
		}
	}
}