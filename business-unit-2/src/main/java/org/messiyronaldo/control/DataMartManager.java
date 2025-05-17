package org.messiyronaldo.control;

import java.io.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import org.json.JSONObject;

public class DataMartManager {
    private static final String BASE_FOLDER = "datamart";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // Estructuras para almacenar datos temporalmente
    private final Map<LocalDate, Map<String, String>> energyData = new HashMap<>();
    private final Map<LocalDate, Map<String, Map<String, String>>> weatherData = new HashMap<>();

    public void saveToDataMart(String eventData) {
        try {
            JSONObject json = new JSONObject(eventData);
            System.out.println("Procesando JSON: " + json.toString());

            // Determinar tipo de evento
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

        // Construir la fila CSV
        StringBuilder row = new StringBuilder();
        row.append(timestamp);
        if (json.has("pricePVPC")) row.append(",").append(json.getDouble("pricePVPC"));
        if (json.has("priceSpot")) row.append(",").append(json.getDouble("priceSpot"));

        // Almacenar en el mapa temporal
        energyData.computeIfAbsent(date, k -> new HashMap<>()).put(hour, row.toString());
    }

    private void processWeatherEvent(JSONObject json) {
        String timestamp = json.getString("predictionTimestamp");
        ZonedDateTime dateTime = ZonedDateTime.parse(timestamp);
        LocalDate date = dateTime.toLocalDate();
        String hour = dateTime.getHour() + ":00";

        // Obtener ubicación
        String location = "unknown";
        if (json.has("location")) {
            if (json.get("location") instanceof JSONObject) {
                location = json.getJSONObject("location").optString("name", "unknown").replaceAll("\\s+", "_");
            } else {
                location = json.optString("location", "unknown").replaceAll("\\s+", "_");
            }
        }

        // Construir la fila CSV
        StringBuilder row = new StringBuilder();
        row.append(timestamp).append(",").append(location);
        if (json.has("temperature")) row.append(",").append(json.getDouble("temperature"));
        if (json.has("humidity")) row.append(",").append(json.getDouble("humidity"));
        if (json.has("cloudiness")) row.append(",").append(json.getDouble("cloudiness"));
        if (json.has("weatherDescription")) row.append(",").append(json.getString("weatherDescription"));
        if (json.has("partOfDay")) row.append(",").append(json.getString("partOfDay"));

        // Almacenar en el mapa temporal
        weatherData.computeIfAbsent(date, k -> new HashMap<>())
                .computeIfAbsent(location, k -> new HashMap<>())
                .put(hour, row.toString());
    }

    public void generateConsolidatedFiles() {
        generateEnergyCSV();
        generateWeatherCSV();
    }

    private void generateEnergyCSV() {
        File dir = new File(BASE_FOLDER);
        if (!dir.exists()) dir.mkdirs();

        File file = new File(dir, "energy_consolidated.csv");

        try (FileWriter writer = new FileWriter(file)) {
            // Escribir cabecera
            writer.append("priceTimestamp,pricePVPC,priceSpot\n");

            // Procesar cada día
            for (LocalDate date : energyData.keySet()) {
                Map<String, String> hoursData = energyData.get(date);

                // Verificar si tenemos las 24 horas
                if (hoursData.size() == 24) {
                    // Ordenar por hora y escribir
                    hoursData.entrySet().stream()
                            .sorted(Map.Entry.comparingByKey())
                            .forEach(entry -> {
                                try {
                                    writer.append(entry.getValue()).append("\n");
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });
                }
            }
        } catch (IOException e) {
            System.err.println("Error al generar CSV de energía: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void generateWeatherCSV() {
        File dir = new File(BASE_FOLDER);
        if (!dir.exists()) dir.mkdirs();

        File file = new File(dir, "weather_consolidated.csv");

        try (FileWriter writer = new FileWriter(file)) {
            // Escribir cabecera
            writer.append("predictionTimestamp,location,temperature,humidity,cloudiness,weatherDescription,partOfDay\n");

            // Procesar cada día
            for (LocalDate date : weatherData.keySet()) {
                Map<String, Map<String, String>> locationsData = weatherData.get(date);

                // Verificar que todas las ubicaciones tengan 24 horas
                boolean allComplete = locationsData.values().stream()
                        .allMatch(hours -> hours.size() == 24);

                if (allComplete) {
                    // Escribir datos para cada ubicación
                    for (String location : locationsData.keySet()) {
                        Map<String, String> hoursData = locationsData.get(location);

                        // Ordenar por hora y escribir
                        hoursData.entrySet().stream()
                                .sorted(Map.Entry.comparingByKey())
                                .forEach(entry -> {
                                    try {
                                        writer.append(entry.getValue()).append("\n");
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                });
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error al generar CSV de clima: " + e.getMessage());
            e.printStackTrace();
        }
    }
}