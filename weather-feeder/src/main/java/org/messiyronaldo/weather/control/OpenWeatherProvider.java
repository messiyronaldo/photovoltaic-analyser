package org.messiyronaldo.weather.control;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;

public class OpenWeatherProvider implements WeatherProvider {
	private static final Logger logger = LoggerFactory.getLogger(OpenWeatherProvider.class);
	private static final String OPENWEATHER_HOURLY_API_ENDPOINT = "https://pro.openweathermap.org/data/2.5/forecast/hourly";
	private final String apiKey;
	private final OkHttpClient httpClient;
	private final JsonWeatherParser weatherParser;

	public OpenWeatherProvider(String apiKey) {
		this(apiKey, null);
	}

	public OpenWeatherProvider(String apiKey, OkHttpClient httpClient) {
		this.apiKey = apiKey;
		this.httpClient = httpClient != null ? httpClient : createConfiguredHttpClient();
		this.weatherParser = new JsonWeatherParser();
		logger.info("OpenWeather provider initialized");
	}

	private OkHttpClient createConfiguredHttpClient() {
		return new OkHttpClient.Builder()
				.connectTimeout(10, TimeUnit.SECONDS)
				.readTimeout(30, TimeUnit.SECONDS)
				.build();
	}

	@Override
	public List<Weather> getWeatherForecasts(Location location) throws IOException {
		String apiUrl = buildWeatherApiUrlForLocation(location);
		logger.debug("Fetching weather data for location: {} ({}, {})",
			location.getName(), location.getLatitude(), location.getLongitude());

		String jsonResponse = fetchWeatherDataFromApi(apiUrl);
		List<Weather> forecasts = weatherParser.parseWeatherData(jsonResponse, location);
		logger.info("Retrieved {} weather forecasts for location: {}", forecasts.size(), location.getName());
		return forecasts;
	}

	private String buildWeatherApiUrlForLocation(Location location) {
		return String.format("%s?lat=%f&lon=%f&units=metric&appid=%s",
				OPENWEATHER_HOURLY_API_ENDPOINT, location.getLatitude(), location.getLongitude(), apiKey);
	}

	private String fetchWeatherDataFromApi(String apiUrl) throws IOException {
		Request request = new Request.Builder().url(apiUrl).build();
		return executeHttpRequestAndGetResponseBody(request);
	}

	private String executeHttpRequestAndGetResponseBody(Request request) throws IOException {
		try (Response response = httpClient.newCall(request).execute()) {
			validateResponse(response);
			return extractResponseBody(response);
		}
	}

	private void validateResponse(Response response) throws IOException {
		if (!response.isSuccessful()) {
			logger.error("API request failed with status: {} - {}", response.code(), response.message());
			throw new IOException("API error: " + response.code() + " " + response.message());
		}
	}

	private String extractResponseBody(Response response) throws IOException {
		if (response.body() == null) {
			logger.error("API response body is null");
			throw new IOException("API response body is null");
		}
		return response.body().string();
	}

	static class JsonWeatherParser {
		private final Gson jsonParser = new Gson();

		public List<Weather> parseWeatherData(String jsonData, Location location) {
			JsonObject rootObject = jsonParser.fromJson(jsonData, JsonObject.class);
			List<Weather> forecastList = new ArrayList<>();
			extractForecasts(rootObject, location, forecastList);
			return forecastList;
		}

		private void extractForecasts(JsonObject rootObject, Location location, List<Weather> results) {
			Instant retrievalTime = Instant.now();
			JsonArray forecastEntries = rootObject.getAsJsonArray("list");

			for (JsonElement forecastElement : forecastEntries) {
				JsonObject forecastEntry = forecastElement.getAsJsonObject();
				Weather forecast = convertToWeather(forecastEntry, location, retrievalTime);
				results.add(forecast);
			}
		}

		private Weather convertToWeather(JsonObject forecastJson, Location location, Instant retrievalTime) {
			return new Weather(
					retrievalTime,
					location,
					extractTimestamp(forecastJson),
					extractTemperature(forecastJson),
					extractHumidity(forecastJson),
					extractWeatherId(forecastJson),
					extractWeatherMain(forecastJson),
					extractWeatherDescription(forecastJson),
					extractCloudiness(forecastJson),
					extractWindSpeed(forecastJson),
					extractRainVolume(forecastJson),
					extractSnowVolume(forecastJson),
					extractDayPeriod(forecastJson),
					"OpenWeatherApi"
			);
		}

		private Instant extractTimestamp(JsonObject forecastJson) {
			long timestampSeconds = forecastJson.get("dt").getAsLong();
			return Instant.ofEpochSecond(timestampSeconds);
		}

		private double extractTemperature(JsonObject forecastJson) {
			JsonObject mainData = forecastJson.getAsJsonObject("main");
			return mainData.get("temp").getAsDouble();
		}

		private int extractHumidity(JsonObject forecastJson) {
			JsonObject mainData = forecastJson.getAsJsonObject("main");
			return mainData.get("humidity").getAsInt();
		}

		private JsonObject extractPrimaryWeather(JsonObject forecastJson) {
			JsonArray weatherConditions = forecastJson.getAsJsonArray("weather");
			return weatherConditions.get(0).getAsJsonObject();
		}

		private int extractWeatherId(JsonObject forecastJson) {
			JsonObject weather = extractPrimaryWeather(forecastJson);
			return weather.get("id").getAsInt();
		}

		private String extractWeatherMain(JsonObject forecastJson) {
			JsonObject weather = extractPrimaryWeather(forecastJson);
			return weather.get("main").getAsString();
		}

		private String extractWeatherDescription(JsonObject forecastJson) {
			JsonObject weather = extractPrimaryWeather(forecastJson);
			return weather.get("description").getAsString();
		}

		private int extractCloudiness(JsonObject forecastJson) {
			return extractNestedValue(forecastJson, "clouds", "all", JsonElement::getAsInt, 0);
		}

		private double extractWindSpeed(JsonObject forecastJson) {
			return extractNestedValue(forecastJson, "wind", "speed", JsonElement::getAsDouble, 0.0);
		}

		private double extractRainVolume(JsonObject forecastJson) {
			return extractPrecipitation(forecastJson, "rain");
		}

		private double extractSnowVolume(JsonObject forecastJson) {
			return extractPrecipitation(forecastJson, "snow");
		}

		private String extractDayPeriod(JsonObject forecastJson) {
			return extractNestedValue(forecastJson, "sys", "pod", JsonElement::getAsString, "d");
		}

		private double extractPrecipitation(JsonObject forecastJson, String type) {
			if (!forecastJson.has(type)) {
				return 0.0;
			}

			JsonObject precipData = forecastJson.getAsJsonObject(type);
			return getPrecipitationAmount(precipData);
		}

		private double getPrecipitationAmount(JsonObject precipData) {
			if (precipData.has("1h")) {
				return precipData.get("1h").getAsDouble();
			}

			if (precipData.has("3h")) {
				return precipData.get("3h").getAsDouble();
			}

			return 0.0;
		}

		private <T> T extractNestedValue(JsonObject parent, String objectName, String fieldName,
										 Function<JsonElement, T> converter, T defaultValue) {
			if (!parent.has(objectName)) {
				return defaultValue;
			}

			JsonObject nestedObject = parent.getAsJsonObject(objectName);
			return extractFieldValue(nestedObject, fieldName, converter, defaultValue);
		}

		private <T> T extractFieldValue(JsonObject jsonObject, String fieldName,
										Function<JsonElement, T> converter, T defaultValue) {
			if (!jsonObject.has(fieldName)) {
				return defaultValue;
			}

			return converter.apply(jsonObject.get(fieldName));
		}
	}
}