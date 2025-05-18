package org.businessunit.control;

import io.javalin.Javalin;
import java.awt.Desktop;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class BusinessUnitApplication {
    private static final String POWER_BI_URL = "https://app.powerbi.com/view?r=eyJrIjoiYmEyYmE1NjItM2JhOS00NDM4LTgzM2UtZGM5YjJhMWY1NDkzIiwidCI6ImIyYmI3MzFjLTQ2MGQtNDIwZi1hNDc1LTNlZDYxNWE4Mjk4NyIsImMiOjh9";

    public static void start() {
        // Cargar datos históricos
        EventStoreReader eventStoreReader = new EventStoreReader();
        eventStoreReader.loadHistoricalEvents("energy");
        eventStoreReader.loadHistoricalEvents("weather");

        // Abrir Power BI automáticamente
        openPowerBIReport();

        // Iniciar servidor (opcional, solo si necesitas endpoints)
        Javalin app = Javalin.create().start(7000);
        app.get("/status", ctx -> ctx.result("Sistema operativo"));
    }

    private static void openPowerBIReport() {
        try {
            if (Desktop.isDesktopSupported() && Desktop.getDesktop().isSupported(Desktop.Action.BROWSE)) {
                Desktop.getDesktop().browse(new URI(POWER_BI_URL));
                System.out.println("Informe de Power BI abierto automáticamente");
            } else {
                // Alternativa para entornos sin GUI
                String os = System.getProperty("os.name").toLowerCase();
                Runtime rt = Runtime.getRuntime();

                if (os.contains("win")) {
                    rt.exec("rundll32 url.dll,FileProtocolHandler " + POWER_BI_URL);
                } else if (os.contains("mac")) {
                    rt.exec("open " + POWER_BI_URL);
                } else if (os.contains("nix") || os.contains("nux")) {
                    rt.exec("xdg-open " + POWER_BI_URL);
                } else {
                    System.out.println("Accede manualmente a: " + POWER_BI_URL);
                }
            }
        } catch (IOException | URISyntaxException e) {
            System.err.println("Error al abrir Power BI: " + e.getMessage());
        }
    }
}