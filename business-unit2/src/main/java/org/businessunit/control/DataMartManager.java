package org.businessunit.control;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import org.json.JSONObject;

public class DataMartManager {
    private static final String BASE_FOLDER = "datamart";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String ENERGY_HEADER = "priceTimestamp,pricePVPC,priceSpot\n";
    private static final String WEATHER_HEADER = "predictionTimestamp,location,temperature,humidity,cloudiness,weatherDescription,partOfDay\n";

    private final Map<LocalDate, Map<String, String>> energyData = new HashMap<>();
    private final Map<LocalDate, Map<String, Map<String, String>>> weatherData = new HashMap<>();

    public void saveToDataMart(String eventData) {
        try {
            JSONObject json = new JSONObject(eventData);
            System.out.println("Procesando JSON: " + json.toString());

            String type = json.has("pricePVPC") ? "energy" : "weather";

            if (type.equals("energy")) {
                processEnergyEvent(json);
            } else {
                processWeatherEvent(json);
            }

        } catch (Exception e) {
            System.err.println("Error al procesar evento: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processEnergyEvent(JSONObject json) {
        String timestamp = json.getString("priceTimestamp");
        ZonedDateTime dateTime = ZonedDateTime.parse(timestamp);
        LocalDate date = dateTime.toLocalDate();
        String hour = dateTime.getHour() + ":00";

        StringBuilder row = new StringBuilder();
        row.append(timestamp);
        if (json.has("pricePVPC")) row.append(",").append(json.getDouble("pricePVPC"));
        if (json.has("priceSpot")) row.append(",").append(json.getDouble("priceSpot"));

        energyData.computeIfAbsent(date, k -> new HashMap<>()).put(hour, row.toString());
    }

    private void processWeatherEvent(JSONObject json) {
        String timestamp = json.getString("predictionTimestamp");
        ZonedDateTime dateTime = ZonedDateTime.parse(timestamp);
        LocalDate date = dateTime.toLocalDate();
        String hour = dateTime.getHour() + ":00";

        String location = "unknown";
        if (json.has("location")) {
            if (json.get("location") instanceof JSONObject) {
                location = json.getJSONObject("location").optString("name", "unknown").replaceAll("\\s+", "_");
            } else {
                location = json.optString("location", "unknown").replaceAll("\\s+", "_");
            }
        }

        StringBuilder row = new StringBuilder();
        row.append(timestamp).append(",").append(location);
        if (json.has("temperature")) row.append(",").append(json.getDouble("temperature"));
        if (json.has("humidity")) row.append(",").append(json.getDouble("humidity"));
        if (json.has("cloudiness")) row.append(",").append(json.getDouble("cloudiness"));
        if (json.has("weatherDescription")) row.append(",").append(json.getString("weatherDescription"));
        if (json.has("partOfDay")) row.append(",").append(json.getString("partOfDay"));

        weatherData.computeIfAbsent(date, k -> new HashMap<>())
                .computeIfAbsent(location, k -> new HashMap<>())
                .put(hour, row.toString());
    }

    public void generateConsolidatedFiles() {
        updateEnergyCSV();
        updateWeatherCSV();
    }

    private void updateEnergyCSV() {
        File dir = new File(BASE_FOLDER);
        if (!dir.exists()) dir.mkdirs();

        File file = new File(dir, "energy_consolidated.csv");
        Map<LocalDate, List<String>> newData = new HashMap<>();

        // Procesar datos nuevos
        for (LocalDate date : energyData.keySet()) {
            Map<String, String> hoursData = energyData.get(date);
            if (hoursData.size() == 24) {
                List<String> sortedRows = hoursData.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
                newData.put(date, sortedRows);
            }
        }

        try {
            if (!file.exists()) {
                // Archivo no existe, crear nuevo con todos los datos
                try (FileWriter writer = new FileWriter(file)) {
                    writer.append(ENERGY_HEADER);
                    newData.values().forEach(rows -> {
                        try {
                            for (String row : rows) {
                                writer.append(row).append("\n");
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                }
            } else {
                // Archivo existe, actualizar solo con datos nuevos o modificados
                Set<String> existingLines = new HashSet<>(Files.readAllLines(file.toPath()));
                existingLines.remove(ENERGY_HEADER.trim()); // Eliminar header del conjunto

                try (FileWriter writer = new FileWriter(file, true)) {
                    for (List<String> rows : newData.values()) {
                        for (String row : rows) {
                            if (!existingLines.contains(row)) {
                                writer.append(row).append("\n");
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error al actualizar CSV de energía: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void updateWeatherCSV() {
        File dir = new File(BASE_FOLDER);
        if (!dir.exists()) dir.mkdirs();

        File file = new File(dir, "weather_consolidated.csv");
        Map<LocalDate, List<String>> newData = new HashMap<>();

        // Procesar datos nuevos
        for (LocalDate date : weatherData.keySet()) {
            Map<String, Map<String, String>> locationsData = weatherData.get(date);
            boolean allComplete = locationsData.values().stream()
                    .allMatch(hours -> hours.size() == 24);

            if (allComplete) {
                List<String> allRows = new ArrayList<>();
                for (String location : locationsData.keySet()) {
                    Map<String, String> hoursData = locationsData.get(location);
                    List<String> sortedRows = hoursData.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());
                    allRows.addAll(sortedRows);
                }
                newData.put(date, allRows);
            }
        }

        try {
            if (!file.exists()) {
                // Archivo no existe, crear nuevo con todos los datos
                try (FileWriter writer = new FileWriter(file)) {
                    writer.append(WEATHER_HEADER);
                    newData.values().forEach(rows -> {
                        try {
                            for (String row : rows) {
                                writer.append(row).append("\n");
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
                }
            } else {
                // Archivo existe, actualizar solo con datos nuevos o modificados
                Set<String> existingLines = new HashSet<>(Files.readAllLines(file.toPath()));
                existingLines.remove(WEATHER_HEADER.trim()); // Eliminar header del conjunto

                try (FileWriter writer = new FileWriter(file, true)) {
                    for (List<String> rows : newData.values()) {
                        for (String row : rows) {
                            if (!existingLines.contains(row)) {
                                writer.append(row).append("\n");
                            }
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error al actualizar CSV de clima: " + e.getMessage());
            e.printStackTrace();
        }
    }
}