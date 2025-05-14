package org.messiyronaldo.control;

import java.io.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import org.json.JSONObject;

public class DataMartManager {
    private static final String BASE_FOLDER = "datamart";

    public void saveToDataMart(String eventData) {
        try {
            JSONObject json = new JSONObject(eventData);
            System.out.println("Procesando JSON: " + json.toString());

            // Seleccionar el timestamp correcto
            String timestamp = json.has("priceTimestamp") ? json.getString("priceTimestamp") :
                    json.has("predictionTimestamp") ? json.getString("predictionTimestamp") : "";

            if (timestamp.isEmpty()) {
                System.err.println("Error: JSON sin campo de timestamp adecuado");
                return;
            }

            // Determinar tipo de evento
            String type = json.has("pricePVPC") ? "energy" : "weather";

            // Extraer ubicación SOLO para eventos de clima
            String location = "";
            if (type.equals("weather") && json.has("location")) {
                if (json.get("location") instanceof JSONObject) {
                    location = json.getJSONObject("location").optString("name", "unknown").replaceAll("\\s+", "_");
                } else {
                    location = json.optString("location", "unknown").replaceAll("\\s+", "_");
                }
            }

            // Parsear fecha
            ZonedDateTime dateTime = ZonedDateTime.parse(timestamp);
            LocalDate date = dateTime.toLocalDate();

            // Crear directorios
            File dir = new File(BASE_FOLDER + File.separator + type);
            if (!dir.exists()) dir.mkdirs();

            // Crear nombre de archivo
            String fileName = type.equals("energy") ? String.format("energy_%s.csv", date) : String.format("%s_%s.csv", location, date);
            File file = new File(dir, fileName);
            boolean fileExists = file.exists();

            try (FileWriter writer = new FileWriter(file, true)) {
                // Escribir cabecera si es un nuevo archivo
                if (!fileExists) {
                    if (type.equals("energy")) {
                        writer.append("priceTimestamp,pricePVPC,priceSpot\n");
                    } else {
                        writer.append("predictionTimestamp,location,temperature,humidity,cloudiness,weatherDescription,partOfDay\n");
                    }
                }

                // Construcción dinámica del contenido
                StringBuilder row = new StringBuilder(timestamp);

                if (type.equals("energy")) {
                    if (json.has("pricePVPC")) row.append(",").append(json.getDouble("pricePVPC"));
                    if (json.has("priceSpot")) row.append(",").append(json.getDouble("priceSpot"));
                } else {
                    row.append(",").append(location);
                    if (json.has("temperature")) row.append(",").append(json.getDouble("temperature"));
                    if (json.has("humidity")) row.append(",").append(json.getDouble("humidity"));
                    if (json.has("cloudiness")) row.append(",").append(json.getDouble("cloudiness"));
                    if (json.has("weatherDescription")) row.append(",").append(json.getString("weatherDescription"));
                    if (json.has("partOfDay")) row.append(",").append(json.getString("partOfDay"));
                }

                // Escribir datos en archivo CSV
                writer.append(row.toString()).append("\n");

            }

        } catch (Exception e) {
            System.err.println("Error al guardar en DataMart: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
