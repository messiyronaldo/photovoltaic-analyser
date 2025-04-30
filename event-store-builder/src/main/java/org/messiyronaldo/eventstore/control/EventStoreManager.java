package org.messiyronaldo.eventstore.control;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.messiyronaldo.eventstore.utils.InstantTypeAdapter;

import java.io.IOException;
import java.time.Instant;

public class EventStoreManager implements EventStore {
	private final Gson gson;
	private final FileEventStore fileEventStore;

	public EventStoreManager() {
		this.gson = new GsonBuilder()
				.registerTypeAdapter(Instant.class, new InstantTypeAdapter())
				.create();
		this.fileEventStore = new FileEventStore();
	}

	@Override
	public void storeEventToFile(String json, String topicName) {
		try {
			fileEventStore.storeEventToFile(json, topicName);
			JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
			System.out.println("Event processed for topic: " + topicName);
		} catch (Exception e) {
			System.err.println("Error handling event: " + e.getMessage());
			// Opcional: Puedes agregar reintentos aquí si es necesario
		}
	}
}