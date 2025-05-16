package org.messiyronaldo.eventstore.control;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.*;
import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

class EventStoreManagerTest {
    private static final String TOPIC = "prediction.Weather";
    private static Path tempDir;
    private EventStoreManager eventStoreManager;
    private static final Logger logger = LoggerFactory.getLogger(EventStoreManagerTest.class);

    @BeforeAll
    static void setupClass() throws IOException {
        tempDir = Files.createTempDirectory("eventstore-test");
        System.setProperty("user.dir", tempDir.toAbsolutePath().toString());
    }

    @AfterAll
    static void cleanupClass() throws IOException {
        if (tempDir != null) {
            Files.walk(tempDir)
                .sorted((a, b) -> b.compareTo(a))
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }

    @BeforeEach
    void setup() {
        eventStoreManager = new EventStoreManager();
    }

    @Test
    void testStoreEventToFile_createsFileAndAvoidsDuplicates() throws Exception {
        String ts = Instant.now().toString();
        String event1 = "{" +
                "\"ts\":\"" + ts + "\"," +
                "\"location\":{\"name\":\"Madrid\",\"latitude\":40.4,\"longitude\":-3.7}," +
                "\"predictionTimestamp\":\"2025-05-16T13:00:00Z\"," +
                "\"temperature\":15.0," +
                "\"humidity\":60," +
                "\"weatherID\":800," +
                "\"weatherMain\":\"Clear\"," +
                "\"weatherDescription\":\"clear sky\"," +
                "\"cloudiness\":0," +
                "\"windSpeed\":2.0," +
                "\"rainVolume\":0.0," +
                "\"snowVolume\":0.0," +
                "\"partOfDay\":\"d\"," +
                "\"ss\":\"OpenWeatherApi\"}";

        // Store first event
        eventStoreManager.storeEventToFile(event1, TOPIC);
        String date = ts.substring(0, 10).replace("-", "");
        Path filePath = tempDir.resolve("eventstore/Weather/OpenWeatherApi/" + date + ".events");
        assertTrue(Files.exists(filePath), "Event file should be created");
        List<String> lines = Files.readAllLines(filePath);
        assertEquals(1, lines.size(), "Should have one event");

        // Store the same event again (should not duplicate)
        eventStoreManager.storeEventToFile(event1, TOPIC);
        lines = Files.readAllLines(filePath);
        assertEquals(1, lines.size(), "Should still have one event (no duplicate)");

        // Store a different event (same ts, different temperature)
        String event2 = event1.replace("15.0", "16.0");
        eventStoreManager.storeEventToFile(event2, TOPIC);
        lines = Files.readAllLines(filePath);
        assertEquals(2, lines.size(), "Should have two events (different data)");
    }
}