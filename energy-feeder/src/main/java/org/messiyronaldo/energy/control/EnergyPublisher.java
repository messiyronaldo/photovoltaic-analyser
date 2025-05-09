package org.messiyronaldo.energy.control;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.messiyronaldo.energy.model.EnergyPrice;
import org.messiyronaldo.energy.utils.InstantTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.time.Instant;
import java.lang.IllegalStateException;

public class EnergyPublisher implements Publisher {
	private static final Logger logger = LoggerFactory.getLogger(EnergyPublisher.class);
	private static final String BROKER_URL = "tcp://localhost:61616";
	private static final String TOPIC_NAME = "prediction.Energy";
	private static final int SESSION_ACKNOWLEDGE_MODE = Session.AUTO_ACKNOWLEDGE;
	private static final boolean TRANSACTED = false;

	private final Gson gson;
	private Connection connection;
	private volatile boolean started = false;

	public EnergyPublisher() {
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
			logger.warn("Energy publisher is already started");
			return;
		}
		initializeConnection();
	}

	private boolean isAlreadyStarted() {
		if (started) {
			logger.warn("Energy publisher is already started");
			return true;
		}
		return false;
	}

	private void initializeConnection() {
		try {
			connection = createConnection();
			connection.start();
			started = true;
			logger.info("Energy publisher started successfully");
		} catch (JMSException e) {
			logger.error("Failed to start energy publisher: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to start energy publisher", e);
		}
	}

	private Connection createConnection() throws JMSException {
		try {
			return new ActiveMQConnectionFactory(BROKER_URL).createConnection();
		} catch (JMSException e) {
			logger.error("Failed to create JMS connection: {}", e.getMessage(), e);
			throw e;
		}
	}

	@Override
	public void publish(EnergyPrice price) {
		validatePublisherState();

		Session session = null;
		MessageProducer producer = null;

		try {
			session = createSession();
			producer = createProducer(session);
			sendEnergyEvent(price, session, producer);
			logPublishedEvent();
		} catch (JMSException e) {
			logger.error("Failed to publish energy price: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to publish energy price", e);
		} finally {
			closeResources(session, producer);
		}
	}

	private void validatePublisherState() {
		if (!started) {
			String error = "Energy publisher is not started";
			logger.error(error);
			throw new IllegalStateException(error);
		}
	}

	private Session createSession() throws JMSException {
		try {
			return connection.createSession(TRANSACTED, SESSION_ACKNOWLEDGE_MODE);
		} catch (JMSException e) {
			logger.error("Failed to create JMS session: {}", e.getMessage(), e);
			throw e;
		}
	}

	private MessageProducer createProducer(Session session) throws JMSException {
		try {
			Destination destination = session.createTopic(TOPIC_NAME);
			return session.createProducer(destination);
		} catch (JMSException e) {
			logger.error("Failed to create message producer: {}", e.getMessage(), e);
			throw e;
		}
	}

	private void sendEnergyEvent(EnergyPrice price, Session session, MessageProducer producer) throws JMSException {
		String json = gson.toJson(price);
		TextMessage message = session.createTextMessage(json);
		producer.send(message);
		logger.debug("Sent energy price event: {}", json);
	}

	private void logPublishedEvent() {
		logger.info("Published energy price event");
	}

	private void closeResources(Session session, MessageProducer producer) {
		if (producer != null) {
			try {
				producer.close();
				logger.debug("Message producer closed");
			} catch (JMSException e) {
				logger.warn("Error closing message producer: {}", e.getMessage());
			}
		}
		if (session != null) {
			try {
				session.close();
				logger.debug("JMS session closed");
			} catch (JMSException e) {
				logger.warn("Error closing JMS session: {}", e.getMessage());
			}
		}
	}

	@Override
	public void close() {
		if (!started) {
			logger.warn("Energy publisher is not started");
			return;
		}

		try {
			if (connection != null) {
				connection.close();
				logger.debug("JMS connection closed");
			}
			started = false;
			logger.info("Energy publisher closed successfully");
		} catch (JMSException e) {
			logger.error("Error closing energy publisher: {}", e.getMessage(), e);
			throw new RuntimeException("Error closing energy publisher", e);
		}
	}
}