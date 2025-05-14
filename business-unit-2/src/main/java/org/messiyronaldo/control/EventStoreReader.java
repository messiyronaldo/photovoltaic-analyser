package org.messiyronaldo.control;

import java.io.*;
import java.util.List;
import java.util.ArrayList;

public class EventStoreReader {
    private static final String ENERGY_EVENTS_PATH = "eventstore/Energy/RedElectricaApi/";
    private static final String WEATHER_EVENTS_PATH = "eventstore/Weather/OpenWeatherApi/";
    private final DataMartManager dataMartManager = new DataMartManager();

    public List<String> loadHistoricalEvents(String eventType) {
        List<String> events = new ArrayList<>();
        String path = eventType.equalsIgnoreCase("energy") ? ENERGY_EVENTS_PATH : WEATHER_EVENTS_PATH;

        File folder = new File(path);
        File[] files = folder.listFiles((dir, name) -> name.endsWith(".events"));

        if (files != null) {
            for (File file : files) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        events.add(line);
                        dataMartManager.saveToDataMart(line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return events;
    }
}
