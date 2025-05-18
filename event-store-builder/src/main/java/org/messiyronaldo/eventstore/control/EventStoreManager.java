package org.messiyronaldo.eventstore.control;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.messiyronaldo.eventstore.utils.InstantTypeAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class EventStoreManager implements EventStore {
	private static final Logger logger = LoggerFactory.getLogger(EventStoreManager.class);
	private final Gson gson;

	public EventStoreManager() {
		this.gson = new GsonBuilder()
				.registerTypeAdapter(Instant.class, new InstantTypeAdapter())
				.create();
		logger.info("Event store manager initialized");
	}

	@Override
	public void storeEventToFile(String json, String topicName) {
		try {
			JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
			String formattedTimestamp = getEventDateFromTs(jsonObject);
			File directory = createDirectory(jsonObject, topicName);
			File file = new File(directory, formattedTimestamp + ".events");

			// Read all existing events
			java.util.List<JsonObject> events = new java.util.ArrayList<>();
			if (file.exists()) {
				try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(file))) {
					String line;
					while ((line = reader.readLine()) != null) {
						JsonObject existingEvent = gson.fromJson(line, JsonObject.class);
						events.add(existingEvent);
					}
				}
			}

			String newEventKey = getEventUniqueKey(jsonObject, topicName);
			boolean found = false;
			for (int i = 0; i < events.size(); i++) {
				JsonObject existingEvent = events.get(i);
				String existingKey = getEventUniqueKey(existingEvent, topicName);
				if (existingKey.equals(newEventKey)) {
					found = true;
					if (!eventsEqualIgnoringTs(existingEvent, jsonObject)) {
						events.set(i, jsonObject); // Replace
						logger.info("Event replaced in file: {}", file.getAbsolutePath());
					} else {
						logger.info("Duplicate event detected, not storing: {}", file.getAbsolutePath());
					}
					break;
				}
			}
			if (!found) {
				events.add(jsonObject);
				//logger.info("Event appended to file: {}", file.getAbsolutePath());
			}

			// Write all events back to the file
			try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) {
				for (JsonObject event : events) {
					gson.toJson(event, writer);
					writer.newLine();
				}
			}
		} catch (IOException e) {
			logger.error("Failed to store event: {}", e.getMessage(), e);
		}
	}

	private String getEventDateFromTs(JsonObject jsonObject) {
		if (!jsonObject.has("ts")) {
			throw new IllegalArgumentException("Event JSON does not contain 'ts' field");
		}
		String ts = jsonObject.get("ts").getAsString();
		Instant instant = Instant.parse(ts);
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		return instant.atOffset(ZoneOffset.UTC).format(formatter);
	}

	private boolean eventsEqualIgnoringTs(JsonObject a, JsonObject b) {
		JsonObject aCopy = a.deepCopy();
		JsonObject bCopy = b.deepCopy();
		aCopy.remove("ts");
		bCopy.remove("ts");
		return aCopy.equals(bCopy);
	}

	private File createDirectory(JsonObject jsonObject, String topicName) throws IOException {
		String topic = topicName.contains(".") ? topicName.substring(topicName.indexOf(".") + 1) : topicName;
		String sourceSystem = getCleanedStringValue(jsonObject);
		String directoryPath = "eventstore/" + topic + "/" + sourceSystem + "/";
		File directory = new File(directoryPath);

		if (!directory.exists() && !directory.mkdirs()) {
			String error = "Failed to create directory: " + directory.getAbsolutePath();
			logger.error(error);
			throw new IOException(error);
		}

		logger.debug("Created/accessed directory: {}", directory.getAbsolutePath());
		return directory;
	}

	private String getCleanedStringValue(JsonObject jsonObject) {
		return jsonObject.get("ss").getAsString().replace("\"", "");
	}

	private String getEventUniqueKey(JsonObject event, String topicName) {
		String topic = topicName.contains(".") ? topicName.substring(topicName.indexOf(".") + 1) : topicName;
		if (topic.equalsIgnoreCase("Energy")) {
			return event.has("priceTimestamp") ? event.get("priceTimestamp").getAsString() : "";
		} else if (topic.equalsIgnoreCase("Weather")) {
			StringBuilder key = new StringBuilder();
			if (event.has("predictionTimestamp")) {
				key.append(event.get("predictionTimestamp").getAsString());
			}
			if (event.has("location")) {
				JsonObject loc = event.getAsJsonObject("location");
				if (loc.has("latitude")) key.append("_").append(loc.get("latitude").getAsDouble());
				if (loc.has("longitude")) key.append("_").append(loc.get("longitude").getAsDouble());
			}
			return key.toString();
		}
		return "";
	}
}