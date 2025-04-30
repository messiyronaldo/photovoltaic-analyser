package org.messiyronaldo;

import org.messiyronaldo.eventstore.control.*;

public class Main {
	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String CLIENT_BASE_ID = "MessiyRonaldo";
	private static final String WEATHER_TOPIC = "prediction.Weather";
	private static final String ENERGY_TOPIC = "prediction.Energy";

	public static void main(String[] args) {
		System.out.println("Starting Event Store Builder...");

		EventStore eventStore = new EventStoreManager();

		Subscriber weatherSubscriber = createSubscriber(eventStore, "Weather");
		Subscriber energySubscriber = createSubscriber(eventStore, "Energy");

		registerShutdownHook(weatherSubscriber, energySubscriber);

		System.out.println("Event Store Builder running and subscribed to topics");
		waitForever();
	}

	private static Subscriber createSubscriber(EventStore eventStore, String type) {
		String topic = type.equals("Weather") ? WEATHER_TOPIC : ENERGY_TOPIC;
		Subscriber subscriber = new SubscriberActiveMQ(
				BROKER_URL,
				CLIENT_BASE_ID + "_" + type,
				CLIENT_BASE_ID + "_" + type + "Sub",
				eventStore);

		subscriber.start();
		subscriber.subscribe(topic);
		System.out.println("Successfully subscribed to: " + topic);
		return subscriber;
	}

	private static void registerShutdownHook(Subscriber... subscribers) {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("Shutting down Event Store Builder...");
			for (Subscriber sub : subscribers) {
				if (sub != null) {
					try {
						sub.close();
						System.out.println("Subscriber closed successfully");
					} catch (Exception e) {
						System.err.println("Error closing subscriber: " + e.getMessage());
					}
				}
			}
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