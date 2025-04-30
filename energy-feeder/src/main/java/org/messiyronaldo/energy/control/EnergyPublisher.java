package org.messiyronaldo.energy.control;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.messiyronaldo.energy.model.EnergyPrice;
import org.messiyronaldo.energy.utils.InstantTypeAdapter;

import javax.jms.*;
import java.time.Instant;

public class EnergyPublisher implements Publisher {
	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String TOPIC_NAME = "prediction.Energy";
	private final Gson gson;
	private Connection connection;

	public EnergyPublisher() {
		this.gson = new GsonBuilder()
				.registerTypeAdapter(Instant.class, new InstantTypeAdapter())
				.create();
	}

	@Override
	public void start() {
		try {
			ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
			connection = factory.createConnection();
			connection.start();
			System.out.println("Energy publisher connected to ActiveMQ");
		} catch (JMSException e) {
			throw new RuntimeException("Failed to initialize energy publisher", e);
		}
	}

	@Override
	public void publish(EnergyPrice energyPrice) {
		validateEvent(energyPrice);

		Session session = null;
		MessageProducer producer = null;

		try {
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createTopic(TOPIC_NAME);
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);

			// El Gson ya serializará todos los campos automáticamente
			String jsonEvent = gson.toJson(energyPrice);
			TextMessage message = session.createTextMessage(jsonEvent);
			producer.send(message);

			System.out.println("Published energy event to topic: " + TOPIC_NAME);
		} catch (JMSException e) {
			throw new RuntimeException("Failed to publish energy data", e);
		} finally {
			closeResources(producer, session);
		}
	}

	private void validateEvent(EnergyPrice price) {
		if (price == null || price.getPriceTimestamp() == null ||
				price.getSourceSystem() == null) {
			throw new IllegalArgumentException("Invalid EnergyPrice: missing required fields");
		}

		// Opcional: Validar que al menos uno de los precios no sea cero
		if (price.getPricePVPC() == 0.0 && price.getPriceSpot() == 0.0) {
			throw new IllegalArgumentException("Invalid EnergyPrice: both prices are zero");
		}
	}

	private void closeResources(MessageProducer producer, Session session) {
		try {
			if (producer != null) producer.close();
		} catch (JMSException e) {
			System.err.println("Error closing producer: " + e.getMessage());
		}

		try {
			if (session != null) session.close();
		} catch (JMSException e) {
			System.err.println("Error closing session: " + e.getMessage());
		}
	}

	@Override
	public void close() {
		try {
			if (connection != null) {
				connection.close();
				System.out.println("Energy publisher closed");
			}
		} catch (JMSException e) {
			throw new RuntimeException("Failed to close energy publisher", e);
		}
	}
}