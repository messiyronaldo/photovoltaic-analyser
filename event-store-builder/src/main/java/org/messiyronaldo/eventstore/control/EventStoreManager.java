package org.messiyronaldo.eventstore.control;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.messiyronaldo.eventstore.utils.InstantTypeAdapter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class EventStoreManager implements EventStore {
	private final Gson gson;

	public EventStoreManager() {
		this.gson = new GsonBuilder()
				.registerTypeAdapter(Instant.class, new InstantTypeAdapter())
				.create();
	}

	@Override
	public void storeEventToFile(String json, String topicName) {
		try {
			JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
			String formattedTimestamp = getCurrentFormattedTimestamp();
			File directory = createDirectory(jsonObject, topicName);
			File file = new File(directory, formattedTimestamp + ".events");
			writeEventToFile(jsonObject, file);
			System.out.println("Event stored successfully at: " + file.getAbsolutePath());
		} catch (IOException e) {
			handleError(e);
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
			throw new IOException("Error creating directory: " + directory.getAbsolutePath());
		}

		return directory;
	}

	private void writeEventToFile(JsonObject jsonObject, File file) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
			gson.toJson(jsonObject, writer);
			writer.newLine();
		}
	}

	private String getCleanedStringValue(JsonObject jsonObject) {
		return jsonObject.get("ss").getAsString().replace("\"", "");
	}

	private void handleError(Exception e) {
		System.err.println("Error handling event: " + e.getMessage());
	}
}