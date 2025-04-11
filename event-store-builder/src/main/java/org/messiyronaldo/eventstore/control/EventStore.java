package org.messiyronaldo.eventstore.control;

public interface EventStore {
	void storeEventToFile(String json, String topicName);
}
