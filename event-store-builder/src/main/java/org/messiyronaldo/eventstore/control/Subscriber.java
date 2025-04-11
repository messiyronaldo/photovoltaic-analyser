package org.messiyronaldo.eventstore.control;

public interface Subscriber {
	void start();
	void subscribe(String topicName);
	void close();
}
