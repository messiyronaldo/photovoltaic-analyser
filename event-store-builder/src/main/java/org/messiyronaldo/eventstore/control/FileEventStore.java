package org.messiyronaldo.eventstore.control;

import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class FileEventStore {
    private static final String BASE_DIR = "eventstore";
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneId.systemDefault());

    public void storeEventToFile(String jsonEvent, String topicName) throws IOException {
        String ss = extractSubsystemFromTopic(topicName);
        Path filePath = buildFilePath(topicName, ss);

        try {
            Files.createDirectories(filePath.getParent());
            Files.write(filePath, (jsonEvent + "\n").getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);

            System.out.println("Event stored to: " + filePath);
        } catch (IOException e) {
            System.err.println("Error writing to file: " + filePath);
            throw e;
        }
    }

    private Path buildFilePath(String topic, String ss) {
        String dateStr = DATE_FORMATTER.format(Instant.now());
        return Paths.get(BASE_DIR, sanitize(topic), sanitize(ss), dateStr + ".events");
    }

    private String extractSubsystemFromTopic(String topic) {
        String[] parts = topic.split("\\.");
        return (parts.length > 1) ? parts[parts.length - 1].toLowerCase() : "default";
    }

    private String sanitize(String input) {
        return input.replaceAll("[^a-zA-Z0-9]", "_");
    }
}