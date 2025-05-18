package org.messiyronaldo.weather.control;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.messiyronaldo.weather.model.Weather;
import org.messiyronaldo.weather.utils.InstantTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.time.Instant;
import java.lang.IllegalStateException;

public class ActiveMQWeatherPublisher implements WeatherPublisher {
	private static final Logger logger = LoggerFactory.getLogger(ActiveMQWeatherPublisher.class);
	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String TOPIC_NAME = "prediction.Weather";
	private static final int SESSION_ACKNOWLEDGE_MODE = Session.AUTO_ACKNOWLEDGE;
	private static final boolean TRANSACTED = false;

	private final Gson gson;
	private Connection connection;
	private volatile boolean started = false;

	public ActiveMQWeatherPublisher() {
		gson = createGsonInstance();
	}

	private Gson createGsonInstance() {
		return new GsonBuilder()
				.registerTypeAdapter(Instant.class, new InstantTypeAdapter())
				.create();
	}

	@Override
	public void start() {
		if (isAlreadyStarted()) {
			return;
		}
		initializeConnection();
	}

	private boolean isAlreadyStarted() {
		return started;
	}

	private void initializeConnection() {
		try {
			ConnectionFactory factory = createConnectionFactory();
			connection = factory.createConnection();
			connection.start();
			started = true;
			logger.info("Weather publisher connected to ActiveMQ");
		} catch (JMSException e) {
			handleConnectionError(e);
		}
	}

	private ConnectionFactory createConnectionFactory() {
		return new ActiveMQConnectionFactory(BROKER_URL);
	}

	private void handleConnectionError(JMSException e) {
		logger.error("Failed to initialize weather publisher: {}", e.getMessage(), e);
		throw new RuntimeException("Failed to initialize weather publisher", e);
	}

	@Override
	public void publish(Weather weather) {
		validatePublisherState();
		Session session = null;
		MessageProducer producer = null;

		try {
			session = createSession();
			producer = createProducer(session);
			sendWeatherEvent(producer, session, weather);
		} catch (JMSException e) {
			handlePublishError(e);
		} finally {
			closeResources(producer, session);
		}
	}

	private void validatePublisherState() {
		if (!started) {
			throw new IllegalStateException("Publisher must be started before publishing messages");
		}
	}

	private Session createSession() throws JMSException {
		return connection.createSession(TRANSACTED, SESSION_ACKNOWLEDGE_MODE);
	}

	private MessageProducer createProducer(Session session) throws JMSException {
		Destination destination = session.createTopic(TOPIC_NAME);
		MessageProducer producer = session.createProducer(destination);
		producer.setDeliveryMode(DeliveryMode.PERSISTENT);
		return producer;
	}

	private void sendWeatherEvent(MessageProducer producer, Session session, Weather weather) throws JMSException {
		String jsonEvent = serializeWeatherEvent(weather);
		TextMessage message = createTextMessage(session, jsonEvent);
		producer.send(message);
		logWeatherEvent(jsonEvent);
	}

	private String serializeWeatherEvent(Weather weather) {
		return gson.toJson(weather);
	}

	private TextMessage createTextMessage(Session session, String jsonEvent) throws JMSException {
		return session.createTextMessage(jsonEvent);
	}

	private void logWeatherEvent(String jsonEvent) {
		logger.info("Published weather event to topic: {}", TOPIC_NAME);
		logger.debug("Weather event content: {}", jsonEvent);
	}

	private void handlePublishError(JMSException e) {
		logger.error("Failed to publish weather data: {}", e.getMessage(), e);
		throw new RuntimeException("Failed to publish weather data", e);
	}

	private void closeResources(MessageProducer producer, Session session) {
		closeProducer(producer);
		closeSession(session);
	}

	private void closeProducer(MessageProducer producer) {
		try {
			if (producer != null) {
				producer.close();
			}
		} catch (JMSException e) {
			logger.warn("Error closing producer: {}", e.getMessage());
		}
	}

	private void closeSession(Session session) {
		try {
			if (session != null) {
				session.close();
			}
		} catch (JMSException e) {
			logger.warn("Error closing session: {}", e.getMessage());
		}
	}

	@Override
	public void close() {
		if (!started) {
			return;
		}
		closeConnection();
	}

	private void closeConnection() {
		try {
			if (connection != null) {
				connection.close();
				started = false;
				logger.info("Weather publisher closed");
			}
		} catch (JMSException e) {
			handleCloseError(e);
		}
	}

	private void handleCloseError(JMSException e) {
		logger.error("Failed to close weather publisher: {}", e.getMessage(), e);
		throw new RuntimeException("Failed to close weather publisher", e);
	}
}