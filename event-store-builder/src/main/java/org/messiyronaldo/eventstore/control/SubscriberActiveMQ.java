package org.messiyronaldo.eventstore.control;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class SubscriberActiveMQ implements Subscriber {
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
	}

	@Override
	public void start() {
		try {
			ConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
			connection = factory.createConnection();
			connection.setClientID(clientId);
			connection.start();
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			System.out.println("Subscriber connected to ActiveMQ at " + brokerUrl);
		} catch (JMSException e) {
			throw new RuntimeException("Failed to start subscriber: " + e.getMessage(), e);
		}
	}

	@Override
	public void subscribe(String topicName) {
		try {
			Topic topic = session.createTopic(topicName);
			consumer = session.createDurableSubscriber(topic, subscriberId);
			consumer.setMessageListener(message -> processMessage(message, topicName));
			System.out.println("Subscribed to topic: " + topicName + " with ID: " + subscriberId);
		} catch (JMSException e) {
			throw new RuntimeException("Failed to subscribe to " + topicName + ": " + e.getMessage(), e);
		}
	}

	private void processMessage(Message message, String topicName) {
		try {
			if (message instanceof TextMessage) {
				String json = ((TextMessage) message).getText();
				System.out.println("Received message from topic: " + topicName);
				eventStore.storeEventToFile(json, topicName);
			}
		} catch (JMSException e) {
			System.err.println("Error processing message: " + e.getMessage());
		}
	}

	@Override
	public void close() {
		try {
			if (consumer != null) consumer.close();
			if (session != null) session.close();
			if (connection != null) connection.close();
			System.out.println("Subscriber closed");
		} catch (JMSException e) {
			throw new RuntimeException("Failed to close subscriber: " + e.getMessage(), e);
		}
	}
}