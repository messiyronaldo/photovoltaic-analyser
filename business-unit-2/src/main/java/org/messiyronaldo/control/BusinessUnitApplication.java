package org.messiyronaldo.control;

import io.javalin.Javalin;
import org.json.JSONObject;

import java.time.ZonedDateTime;
import java.util.Objects;

public class BusinessUnitApplication {
    public static void start() {
        RecommendationService recommendationService = new RecommendationService();
        EventStoreReader eventStoreReader = new EventStoreReader();

        // Cargar eventos históricos
        eventStoreReader.loadHistoricalEvents("energy");
        eventStoreReader.loadHistoricalEvents("weather");

        Javalin app = Javalin.create().start(7000);

        app.get("/recommendation", ctx -> {
            double pricePVPC = Double.parseDouble(Objects.requireNonNull(ctx.queryParam("pricePVPC")));
            double cloudiness = Double.parseDouble(Objects.requireNonNull(ctx.queryParam("cloudiness")));
            String partOfDay = ctx.queryParam("partOfDay"); // Por defecto día
            String recommendation = recommendationService.generateRecommendation(pricePVPC, cloudiness, partOfDay);

            JSONObject response = new JSONObject();
            response.put("recommendation", recommendation);
            response.put("timestamp", ZonedDateTime.now().toString());

            ctx.contentType("application/json");
            ctx.result(response.toString());
        });
    }
}