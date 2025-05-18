package org.messiyronaldo.energy.control;

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

public class REEEnergyProvider implements EnergyPricesProvider {
	private static final String BASE_URL = "https://apidatos.ree.es/es/datos/mercados/precios-mercados-tiempo-real";
	private static final String SOURCE_SYSTEM = "RedElectricaApi";
	private static final ZoneId SPAIN_ZONE_ID = ZoneId.of("Europe/Madrid");
	private final OkHttpClient client;
	private final Gson gson;

	public REEEnergyProvider(OkHttpClient client) {
		this.client = client != null ? client : createDefaultClient();
		this.gson = new Gson();
	}

	public REEEnergyProvider() {
		this(null);
	}

	private OkHttpClient createDefaultClient() {
		return new OkHttpClient.Builder()
				.connectTimeout(10, TimeUnit.SECONDS)
				.readTimeout(30, TimeUnit.SECONDS)
				.build();
	}

	@Override
	public List<EnergyPrice> getEnergyPrices(LocalDate date) throws IOException {
		String url = buildApiUrl(date);
		//logQueryUrl(url);

		String jsonResponse = fetchDataFromApi(url);
		return parseEnergyPrices(jsonResponse);
	}

	private String buildApiUrl(LocalDate date) {
		String startDate = formatDateParameter(date, "T00:00");
		String endDate = formatDateParameter(date, "T23:59");

		return BASE_URL +
				"?start_date=" + urlEncode(startDate) +
				"&end_date=" + urlEncode(endDate) +
				"&time_trunc=hour";
	}

	private String formatDateParameter(LocalDate date, String timeSuffix) {
		return date.toString() + timeSuffix;
	}

	private String urlEncode(String text) {
		return text.replace(":", "%3A");
	}

	private void logQueryUrl(String url) {
		System.out.println("Query URL: " + url);
	}

	private String fetchDataFromApi(String url) throws IOException {
		Request request = buildApiRequest(url);

		try (Response response = client.newCall(request).execute()) {
			validateResponse(response);
			return extractResponseBody(response);
		}
	}

	private Request buildApiRequest(String url) {
		return new Request.Builder()
				.url(url)
				.header("Accept", "application/json")
				.build();
	}

	private void validateResponse(Response response) throws IOException {
		if (!response.isSuccessful()) {
			throw new IOException("API error: " + response.code() + " " + response.message());
		}
	}

	private String extractResponseBody(Response response) throws IOException {
		if (response.body() == null) {
			throw new IOException("Empty response body");
		}

		return response.body().string();
	}

	private List<EnergyPrice> parseEnergyPrices(String jsonResponse) {
		JsonObject rootObject = gson.fromJson(jsonResponse, JsonObject.class);
		JsonArray includedArray = rootObject.getAsJsonArray("included");

		if (includedArray == null || includedArray.isEmpty()) {
			return new ArrayList<>();
		}

		Map<String, double[]> pricesByHour = extractPricesByHour(includedArray);
		return createEnergyPriceList(pricesByHour);
	}

	private Map<String, double[]> extractPricesByHour(JsonArray includedArray) {
		Map<String, double[]> pricesByHour = new HashMap<>();

		for (JsonElement includedElement : includedArray) {
			JsonObject includedObject = includedElement.getAsJsonObject();
			String priceType = includedObject.get("type").getAsString();

			// Determine market type index: 0 for PVPC, 1 for SPOT
			int marketTypeIndex = determineMarketTypeIndex(priceType);
			if (marketTypeIndex == -1) continue; // Skip other types

			processPriceValues(includedObject, marketTypeIndex, pricesByHour);
		}

		return pricesByHour;
	}

	private int determineMarketTypeIndex(String priceType) {
		if (priceType.equals("PVPC")) {
			return 0; // PVPC index
		} else if (priceType.equals("Precio mercado spot")) {
			return 1; // SPOT index
		}
		return -1; // Unknown type
	}

	private void processPriceValues(JsonObject includedObject, int marketTypeIndex,
									Map<String, double[]> pricesByHour) {
		JsonObject attributes = includedObject.getAsJsonObject("attributes");
		JsonArray values = attributes.getAsJsonArray("values");

		for (JsonElement valueElement : values) {
			JsonObject valueObject = valueElement.getAsJsonObject();
			String datetime = valueObject.get("datetime").getAsString();
			double price = valueObject.get("value").getAsDouble();

			storePriceInMap(datetime, price, marketTypeIndex, pricesByHour);
		}
	}

	private void storePriceInMap(String datetime, double price, int marketTypeIndex,
								 Map<String, double[]> pricesByHour) {
		if (!pricesByHour.containsKey(datetime)) {
			pricesByHour.put(datetime, new double[2]); // [PVPC, SPOT]
		}

		double[] priceArray = pricesByHour.get(datetime);
		priceArray[marketTypeIndex] = price;
	}

	private List<EnergyPrice> createEnergyPriceList(Map<String, double[]> pricesByHour) {
		List<EnergyPrice> prices = new ArrayList<>();
		Instant currentTimestamp = Instant.now();
		DateTimeFormatter formatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

		for (Map.Entry<String, double[]> entry : pricesByHour.entrySet()) {
			String datetimeStr = entry.getKey();
			double[] priceValues = entry.getValue();

			Instant priceTimestamp = LocalDateTime.parse(datetimeStr, formatter)
					.atZone(SPAIN_ZONE_ID)
					.toInstant();

			EnergyPrice energyPrice = new EnergyPrice(
					currentTimestamp,
					priceTimestamp,
					priceValues[0],  // pricePVPC
					priceValues[1],  // priceSpot
					SOURCE_SYSTEM
			);

			prices.add(energyPrice);
		}

		return prices;
	}
}