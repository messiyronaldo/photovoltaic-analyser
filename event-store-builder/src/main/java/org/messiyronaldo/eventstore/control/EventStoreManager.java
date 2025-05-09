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
			String formattedTimestamp = getCurrentFormattedTimestamp();
			File directory = createDirectory(jsonObject, topicName);
			File file = new File(directory, formattedTimestamp + ".events");
			writeEventToFile(jsonObject, file);
			logger.info("Event stored successfully at: {}", file.getAbsolutePath());
		} catch (IOException e) {
			logger.error("Failed to store event: {}", e.getMessage(), e);
		}
	}

	private String getCurrentFormattedTimestamp() {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		return Instant.now().atOffset(ZoneOffset.UTC).format(formatter);
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

	private void writeEventToFile(JsonObject jsonObject, File file) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
			gson.toJson(jsonObject, writer);
			writer.newLine();
			logger.debug("Wrote event to file: {}", file.getName());
		}
	}

	private String getCleanedStringValue(JsonObject jsonObject) {
		return jsonObject.get("ss").getAsString().replace("\"", "");
	}
}