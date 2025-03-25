package org.messiyronaldo.energy.provider;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.messiyronaldo.energy.model.EnergyPrice;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Proveedor de datos de precios de energía que utiliza la API de Red Eléctrica Española
 */
public class REEEnergyProvider implements EnergyPricesProvider {
	private static final String BASE_URL = "https://apidatos.ree.es/es/datos/mercados/precios-mercados-tiempo-real";
	private final OkHttpClient client;
	private final Gson gson;

	/**
	 * Constructor con cliente HTTP opcional
	 *
	 * @param client Cliente HTTP preconfigurardo (opcional)
	 */
	public REEEnergyProvider(OkHttpClient client) {
		this.client = client != null ? client : createDefaultClient();
		this.gson = new Gson();
	}

	/**
	 * Constructor simple
	 */
	public REEEnergyProvider() {
		this(null);
	}

	/**
	 * Crea un cliente HTTP por defecto con timeouts adecuados
	 */
	private OkHttpClient createDefaultClient() {
		return new OkHttpClient.Builder()
				.connectTimeout(10, TimeUnit.SECONDS)
				.readTimeout(30, TimeUnit.SECONDS)
				.build();
	}

	/**
	 * Obtiene precios de energía para una fecha específica
	 *
	 * @param date Fecha para la que se quieren obtener los precios
	 * @return Lista de precios de energía
	 */
	public List<EnergyPrice> getEnergyPrices(LocalDate date) throws IOException {
		// Construir URL con parámetros para obtener los datos del día completo
		// Formato requerido: https://apidatos.ree.es/es/datos/mercados/precios-mercados-tiempo-real?start_date=2025-03-19T00%3A00&end_date=2025-03-19T23%3A59&time_trunc=hour

		String startDate = date.toString() + "T00:00";
		String endDate = date.toString() + "T23:59";

		StringBuilder urlBuilder = new StringBuilder(BASE_URL);
		urlBuilder.append("?start_date=").append(startDate.replace(":", "%3A"));
		urlBuilder.append("&end_date=").append(endDate.replace(":", "%3A"));
		urlBuilder.append("&time_trunc=hour");

		String url = urlBuilder.toString();
		System.out.println("URL de consulta: " + url);

		// Realizar petición HTTP
		String jsonResponse = fetchDataFromApi(url);

		// Parsear respuesta
		return parseEnergyPrices(jsonResponse);
	}

	/**
	 * Realiza la solicitud HTTP a la API
	 */
	private String fetchDataFromApi(String url) throws IOException {
		Request request = new Request.Builder()
				.url(url)
				.header("Accept", "application/json")
				.build();

		try (Response response = client.newCall(request).execute()) {
			if (!response.isSuccessful()) {
				throw new IOException("Error en la respuesta: " + response.code() + " " + response.message());
			}

			if (response.body() == null) {
				throw new IOException("Respuesta sin contenido");
			}

			return response.body().string();
		}
	}

	/**
	 * Parsea los datos JSON de la API
	 */
	private List<EnergyPrice> parseEnergyPrices(String jsonResponse) {
		List<EnergyPrice> prices = new ArrayList<>();
		Instant currentTimestamp = Instant.now();

		JsonObject rootObject = gson.fromJson(jsonResponse, JsonObject.class);
		JsonArray includedArray = rootObject.getAsJsonArray("included");

		if (includedArray == null || includedArray.size() == 0) {
			return prices;
		}

		// Mapeo para almacenar precios PVPC y spot por hora
		Map<String, double[]> pricesByHour = new HashMap<>();

		// Procesar cada tipo de precio (PVPC y Spot)
		for (JsonElement includedElement : includedArray) {
			JsonObject includedObject = includedElement.getAsJsonObject();
			String priceType = includedObject.get("type").getAsString();

			JsonObject attributes = includedObject.getAsJsonObject("attributes");
			JsonArray values = attributes.getAsJsonArray("values");

			// Determinar el tipo de mercado
			String marketType;
			if (priceType.equals("PVPC")) {
				marketType = "PVPC";
			} else if (priceType.equals("Precio mercado spot")) {
				marketType = "SPOT";
			} else {
				// Ignorar otros tipos
				continue;
			}

			// Procesar valores
			for (JsonElement valueElement : values) {
				JsonObject valueObject = valueElement.getAsJsonObject();
				String datetime = valueObject.get("datetime").getAsString();
				double price = valueObject.get("value").getAsDouble();

				// Guardar en el mapa
				if (!pricesByHour.containsKey(datetime)) {
					pricesByHour.put(datetime, new double[2]); // [PVPC, SPOT]
				}

				double[] priceArray = pricesByHour.get(datetime);
				if (marketType.equals("PVPC")) {
					priceArray[0] = price;
				} else {
					priceArray[1] = price;
				}
			}
		}

		// Crear objetos EnergyPrice para cada hora con ambos tipos de precios
		ZoneId zoneId = ZoneId.of("Europe/Madrid");
		DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

		for (Map.Entry<String, double[]> entry : pricesByHour.entrySet()) {
			String datetimeStr = entry.getKey();
			double[] priceValues = entry.getValue();

			Instant priceTimestamp = LocalDateTime.parse(datetimeStr, formatter)
					.atZone(zoneId)
					.toInstant();

			// Crear un único objeto EnergyPrice con ambos precios
			EnergyPrice energyPrice = new EnergyPrice(
					currentTimestamp,
					priceTimestamp,
					priceValues[0],  // pricePVPC
					priceValues[1]   // priceSpot
			);
			prices.add(energyPrice);
		}

		return prices;
	}
}