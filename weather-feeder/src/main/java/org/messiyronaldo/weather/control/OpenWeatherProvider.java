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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;

public class OpenWeatherProvider implements WeatherProvider {
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
	}

	private OkHttpClient createConfiguredHttpClient() {
		return new OkHttpClient.Builder()
				.connectTimeout(10, TimeUnit.SECONDS)
				.readTimeout(30, TimeUnit.SECONDS)
				.build();
	}

	@Override
	public List<Weather> getHourlyForecast(Location location) throws IOException {
		String apiUrl = buildWeatherApiUrlForLocation(location);
		String jsonResponse = fetchWeatherDataFromApi(apiUrl);
		return weatherParser.parseWeatherData(jsonResponse, location);
	}

	private String buildWeatherApiUrlForLocation(Location location) {
		return String.format("%s?lat=%f&lon=%f&units=metric&appid=%s",
				OPENWEATHER_HOURLY_API_ENDPOINT, location.getLatitude(), location.getLongitude(), apiKey);
	}

	private String fetchWeatherDataFromApi(String apiUrl) throws IOException {
		Request httpRequest = createHttpGetRequest(apiUrl);
		return executeHttpRequestAndGetResponseBody(httpRequest);
	}

	private Request createHttpGetRequest(String url) {
		return new Request.Builder()
				.url(url)
				.build();
	}

	private String executeHttpRequestAndGetResponseBody(Request request) throws IOException {
		try (Response response = httpClient.newCall(request).execute()) {
			validateHttpResponse(response);

			if (response.body() == null) {
				throw new IOException("API response body is null");
			}

			return response.body().string();
		}
	}

	private void validateHttpResponse(Response response) throws IOException {
		if (!response.isSuccessful()) {
			throw new IOException("API response error: " + response.code() + " " + response.message());
		}
	}

	static class JsonWeatherParser {
		private final Gson jsonParser = new Gson();

		public List<Weather> parseWeatherData(String jsonData, Location location) {
			JsonObject rootJsonObject = parseJsonString(jsonData);
			List<Weather> forecastList = new ArrayList<>();
			extractForecastsFromJson(rootJsonObject, location, forecastList);
			return forecastList;
		}

		private JsonObject parseJsonString(String jsonData) {
			return jsonParser.fromJson(jsonData, JsonObject.class);
		}

		private void extractForecastsFromJson(JsonObject rootJsonObject, Location location, List<Weather> weatherResults) {
			Instant dataRetrievalTime = Instant.now();
			JsonArray forecastEntries = rootJsonObject.getAsJsonArray("list");

			for (JsonElement forecastElement : forecastEntries) {
				JsonObject forecastEntry = forecastElement.getAsJsonObject();
				Weather weatherForecast = convertJsonObjectToWeather(forecastEntry, location, dataRetrievalTime);
				weatherResults.add(weatherForecast);
			}
		}

		private Weather convertJsonObjectToWeather(JsonObject forecastJson, Location location, Instant retrievalTime) {
			return new Weather(
					retrievalTime,
					location,
					extractForecastTimestamp(forecastJson),
					extractTemperatureCelsius(forecastJson),
					extractHumidityPercentage(forecastJson),
					extractWeatherConditionCode(forecastJson),
					extractWeatherConditionMain(forecastJson),
					extractWeatherConditionDescription(forecastJson),
					extractCloudCoverPercentage(forecastJson),
					extractWindSpeedMetersPerSecond(forecastJson),
					extractRainVolumeMillimeters(forecastJson),
					extractSnowVolumeMillimeters(forecastJson),
					extractDayPeriodIndicator(forecastJson),
					"OpenWeatherApi"
			);
		}

		private Instant extractForecastTimestamp(JsonObject forecastJson) {
			long timestampSeconds = forecastJson.get("dt").getAsLong();
			return Instant.ofEpochSecond(timestampSeconds);
		}

		private double extractTemperatureCelsius(JsonObject forecastJson) {
			JsonObject mainDataSection = forecastJson.getAsJsonObject("main");
			return mainDataSection.get("temp").getAsDouble();
		}

		private int extractHumidityPercentage(JsonObject forecastJson) {
			JsonObject mainDataSection = forecastJson.getAsJsonObject("main");
			return mainDataSection.get("humidity").getAsInt();
		}

		private JsonObject extractPrimaryWeatherCondition(JsonObject forecastJson) {
			JsonArray weatherConditions = forecastJson.getAsJsonArray("weather");
			return weatherConditions.get(0).getAsJsonObject();
		}

		private int extractWeatherConditionCode(JsonObject forecastJson) {
			JsonObject weatherCondition = extractPrimaryWeatherCondition(forecastJson);
			return weatherCondition.get("id").getAsInt();
		}

		private String extractWeatherConditionMain(JsonObject forecastJson) {
			JsonObject weatherCondition = extractPrimaryWeatherCondition(forecastJson);
			return weatherCondition.get("main").getAsString();
		}

		private String extractWeatherConditionDescription(JsonObject forecastJson) {
			JsonObject weatherCondition = extractPrimaryWeatherCondition(forecastJson);
			return weatherCondition.get("description").getAsString();
		}

		private int extractCloudCoverPercentage(JsonObject forecastJson) {
			return extractNestedJsonValue(forecastJson, "clouds", "all", JsonElement::getAsInt, 0);
		}

		private double extractWindSpeedMetersPerSecond(JsonObject forecastJson) {
			return extractNestedJsonValue(forecastJson, "wind", "speed", JsonElement::getAsDouble, 0.0);
		}

		private double extractRainVolumeMillimeters(JsonObject forecastJson) {
			return extractPrecipitationVolume(forecastJson, "rain");
		}

		private double extractSnowVolumeMillimeters(JsonObject forecastJson) {
			return extractPrecipitationVolume(forecastJson, "snow");
		}

		private String extractDayPeriodIndicator(JsonObject forecastJson) {
			return extractNestedJsonValue(forecastJson, "sys", "pod", JsonElement::getAsString, "d");
		}

		private double extractPrecipitationVolume(JsonObject forecastJson, String precipitationType) {
			if (!forecastJson.has(precipitationType)) {
				return 0.0;
			}

			JsonObject precipitationData = forecastJson.getAsJsonObject(precipitationType);
			return extractHourlyPrecipitationAmount(precipitationData);
		}

		private double extractHourlyPrecipitationAmount(JsonObject precipitationData) {
			if (precipitationData.has("1h")) {
				return precipitationData.get("1h").getAsDouble();
			}

			if (precipitationData.has("3h")) {
				return precipitationData.get("3h").getAsDouble();
			}

			return 0.0;
		}

		private <T> T extractNestedJsonValue(JsonObject parentJson, String objectName, String fieldName,
											 Function<JsonElement, T> valueConverter, T defaultValue) {
			if (!parentJson.has(objectName)) {
				return defaultValue;
			}

			JsonObject nestedObject = parentJson.getAsJsonObject(objectName);
			return extractJsonFieldValue(nestedObject, fieldName, valueConverter, defaultValue);
		}

		private <T> T extractJsonFieldValue(JsonObject jsonObject, String fieldName,
											Function<JsonElement, T> valueConverter, T defaultValue) {
			if (!jsonObject.has(fieldName)) {
				return defaultValue;
			}

			return valueConverter.apply(jsonObject.get(fieldName));
		}
	}
}