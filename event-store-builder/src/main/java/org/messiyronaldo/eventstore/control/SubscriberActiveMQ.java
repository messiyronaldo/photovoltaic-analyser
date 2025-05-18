package org.messiyronaldo.eventstore.control;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class SubscriberActiveMQ implements Subscriber {
	private static final Logger logger = LoggerFactory.getLogger(SubscriberActiveMQ.class);
	private final String brokerUrl;
	private final String clientId;
	private final String subscriberId;
	private final EventStore eventStore;
	private Connection connection;
	private Session session;
	private MessageConsumer consumer;

	public SubscriberActiveMQ(String brokerUrl, String clientId, String subscriberId, EventStore eventStore) {
		this.brokerUrl = brokerUrl;
		this.clientId = clientId;
		this.subscriberId = subscriberId;
		this.eventStore = eventStore;
		logger.info("Subscriber initialized with client ID: {} and subscriber ID: {}", clientId, subscriberId);
	}

	@Override
	public void start() {
		try {
			ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
			connection = factory.createConnection();
			connection.setClientID(clientId);
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			logger.info("Subscriber connected to ActiveMQ at {}", brokerUrl);
		} catch (JMSException e) {
			logger.error("Failed to start subscriber: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to start subscriber", e);
		}
	}

	@Override
	public void subscribe(String topicName) {
		try {
			Topic topic = session.createTopic(topicName);
			consumer = session.createDurableSubscriber(topic, subscriberId);
			consumer.setMessageListener(message -> processMessage(message, topicName));
			logger.info("Subscribed to topic: {} with ID: {}", topicName, subscriberId);
		} catch (JMSException e) {
			logger.error("Failed to subscribe to {}: {}", topicName, e.getMessage(), e);
			throw new RuntimeException("Failed to subscribe to " + topicName, e);
		}
	}

	private void processMessage(Message message, String topicName) {
		try {
			if (message instanceof TextMessage) {
				String json = ((TextMessage) message).getText();
				logger.debug("Received message from topic: {}", topicName);
				eventStore.storeEventToFile(json, topicName);
			}
		} catch (JMSException e) {
			logger.error("Error processing message: {}", e.getMessage(), e);
		}
	}

	@Override
	public void close() {
		try {
			if (consumer != null) {
				consumer.close();
				logger.debug("Message consumer closed");
			}
			if (session != null) {
				session.close();
				logger.debug("JMS session closed");
			}
			if (connection != null) {
				connection.close();
				logger.debug("JMS connection closed");
			}
			logger.info("Subscriber closed successfully");
		} catch (JMSException e) {
			logger.error("Failed to close subscriber: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to close subscriber", e);
		}
	}
}