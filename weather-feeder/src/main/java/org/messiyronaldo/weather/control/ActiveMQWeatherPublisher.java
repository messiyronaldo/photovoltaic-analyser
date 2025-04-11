package org.messiyronaldo.weather.control;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.messiyronaldo.weather.model.Weather;
import org.messiyronaldo.weather.utils.InstantTypeAdapter;

import javax.jms.*;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ActiveMQWeatherPublisher implements WeatherPublisher {
	private static final Logger logger = Logger.getLogger(WeatherPublisher.class.getName());
	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String TOPIC_NAME = "prediction.Weather";
	private final Gson gson;
	private Connection connection;

	public ActiveMQWeatherPublisher() {
		gson = new GsonBuilder()
				.registerTypeAdapter(Instant.class, new InstantTypeAdapter())
				.create();
	}

	@Override
	public void start() {
		try {
			ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
			connection = factory.createConnection();
			connection.start();
			logger.info("Weather publisher connected to ActiveMQ");
		} catch (JMSException e) {
			throw new RuntimeException("Failed to initialize weather publisher", e);
		}
	}

	@Override
	public void publish(Weather weather) {
		Session session = null;
		MessageProducer producer = null;

		try {
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createTopic(TOPIC_NAME);
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);

			String jsonEvent = gson.toJson(weather);

			TextMessage message = session.createTextMessage(jsonEvent);
			producer.send(message);

			logger.info("Published weather event to topic: " + TOPIC_NAME);
		} catch (JMSException e) {
			throw new RuntimeException("Failed to publish weather data", e);
		} finally {
			closeResources(producer, session);
		}
	}

	private void closeResources(MessageProducer producer, Session session) {
		try {
			if (producer != null) {
				producer.close();
			}
		} catch (JMSException e) {
			logger.log(Level.WARNING, "Error closing producer", e);
		}

		try {
			if (session != null) {
				session.close();
			}
		} catch (JMSException e) {
			logger.log(Level.WARNING, "Error closing session", e);
		}
	}

	@Override
	public void close() {
		try {
			if (connection != null) {
				connection.close();
				logger.info("Weather publisher closed");
			}
		} catch (JMSException e) {
			throw new RuntimeException("Failed to close weather publisher", e);
		}
	}
}