package org.messiyronaldo;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;

/**
 * Proveedor de datos meteorológicos que utiliza la API OpenWeather
 */
public class OpenWeatherProvider {
	private static final String BASE_URL = "https://pro.openweathermap.org/data/2.5/forecast/hourly";
	private final String apiKey;
	private final OkHttpClient client;
	private final WeatherDataParser parser;

	/**
	 * Constructor con dependencias
	 *
	 * @param apiKey Clave de API para OpenWeather
	 * @param client Cliente HTTP preconfigurardo (opcional)
	 */
	public OpenWeatherProvider(String apiKey, OkHttpClient client) {
		this.apiKey = apiKey;
		this.client = client != null ? client : createDefaultClient();
		this.parser = new GsonWeatherParser();
	}

	/**
	 * Constructor simple
	 *
	 * @param apiKey Clave de API para OpenWeather
	 */
	public OpenWeatherProvider(String apiKey) {
		this(apiKey, null);
	}

	/**
	 * Crea un cliente HTTP por defecto con timeouts adecuados
	 *
	 * @return Cliente OkHttp configurado
	 */
	private OkHttpClient createDefaultClient() {
		return new OkHttpClient.Builder()
				.connectTimeout(10, TimeUnit.SECONDS)
				.readTimeout(30, TimeUnit.SECONDS)
				.build();
	}

	/**
	 * Obtiene pronóstico del clima por hora para una ubicación específica
	 *
	 * @param location Objeto con la información de ubicación
	 * @return Lista de objetos Weather con el pronóstico por horas
	 * @throws IOException Si hay un error en la comunicación con la API
	 */
	public List<Weather> getHourlyForecast(Location location) throws IOException {
		String url = buildApiUrl(location);
		String jsonResponse = fetchDataFromApi(url);

		return parser.parseWeatherData(jsonResponse, location);
	}

	/**
	 * Construye la URL para la API
	 *
	 * @param location Ubicación para la consulta
	 * @return URL completa para la solicitud API
	 */
	private String buildApiUrl(Location location) {
		return String.format("%s?lat=%f&lon=%f&units=metric&appid=%s",
				BASE_URL, location.getLatitude(), location.getLongitude(), apiKey);
	}

	/**
	 * Realiza la solicitud HTTP a la API
	 *
	 * @param url URL completa para la solicitud
	 * @return Respuesta JSON como String
	 * @throws IOException Si hay un error en la comunicación
	 */
	private String fetchDataFromApi(String url) throws IOException {
		Request request = new Request.Builder()
				.url(url)
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
	 * Interfaz para parsear datos meteorológicos
	 */
	interface WeatherDataParser {
		/**
		 * Parsea los datos JSON de la API y crea objetos Weather
		 *
		 * @param jsonData Respuesta JSON como String
		 * @param location Ubicación para la cual se solicitó el pronóstico
		 * @return Lista de objetos Weather
		 */
		List<Weather> parseWeatherData(String jsonData, Location location);
	}

	/**
	 * Implementación del parser de datos meteorológicos usando Gson
	 */
	static class GsonWeatherParser implements WeatherDataParser {
		private final Gson gson = new Gson();

		@Override
		public List<Weather> parseWeatherData(String jsonData, Location location) {
			List<Weather> weatherList = new ArrayList<>();
			JsonObject rootObject = gson.fromJson(jsonData, JsonObject.class);

			// Actualizar nombre de la ubicación si está disponible
			updateLocationName(rootObject, location);

			// Timestamp actual común para todos los pronósticos
			Instant currentTimestamp = Instant.now();

			// Procesar lista de pronósticos
			JsonArray forecastList = rootObject.getAsJsonArray("list");
			for (JsonElement element : forecastList) {
				JsonObject forecastObject = element.getAsJsonObject();
				Weather weather = createWeatherFromJson(forecastObject, location, currentTimestamp);
				weatherList.add(weather);
			}

			return weatherList;
		}

		/**
		 * Actualiza el nombre de la ubicación con datos de la API
		 */
		private void updateLocationName(JsonObject rootObject, Location location) {
			if (rootObject.has("city")) {
				JsonObject cityObject = rootObject.getAsJsonObject("city");
				if (cityObject.has("name")) {
					String cityName = cityObject.get("name").getAsString();
					location.setName(cityName);
				}
			}
		}

		/**
		 * Crea un objeto Weather a partir de un objeto JSON de pronóstico
		 */
		private Weather createWeatherFromJson(JsonObject forecastObject, Location location, Instant currentTimestamp) {
			// Obtener timestamp de la predicción
			long predictionTimestampSeconds = forecastObject.get("dt").getAsLong();
			Instant predictionTimestamp = Instant.ofEpochSecond(predictionTimestampSeconds);

			// Extraer valores principales del pronóstico
			JsonObject mainObject = forecastObject.getAsJsonObject("main");
			double temperature = mainObject.get("temp").getAsDouble();
			int humidity = mainObject.get("humidity").getAsInt();

			// Extraer información del clima (primera condición)
			JsonArray weatherArray = forecastObject.getAsJsonArray("weather");
			JsonObject weatherObject = weatherArray.get(0).getAsJsonObject();
			int weatherID = weatherObject.get("id").getAsInt();
			String weatherMain = weatherObject.get("main").getAsString();
			String weatherDescription = weatherObject.get("description").getAsString();

			// Extraer información de nubes
			int cloudiness = extractJsonValue(forecastObject, "clouds", "all", JsonElement::getAsInt, 0);

			// Extraer información del viento
			double windSpeed = extractJsonValue(forecastObject, "wind", "speed", JsonElement::getAsDouble, 0.0);

			// Extraer volumen de lluvia (puede no existir)
			double rainVolume = extractPrecipitationVolume(forecastObject, "rain");

			// Extraer volumen de nieve (puede no existir)
			double snowVolume = extractPrecipitationVolume(forecastObject, "snow");

			// Extraer parte del día
			String partOfDay = extractJsonValue(forecastObject, "sys", "pod", JsonElement::getAsString, "d");

			// Crear y devolver objeto Weather
			return new Weather(
					currentTimestamp,
					location,
					predictionTimestamp,
					temperature,
					humidity,
					weatherID,
					weatherMain,
					weatherDescription,
					cloudiness,
					windSpeed,
					rainVolume,
					snowVolume,
					partOfDay
			);
		}

		/**
		 * Extrae valor de precipitación (lluvia o nieve) del pronóstico
		 */
		private double extractPrecipitationVolume(JsonObject forecastObject, String precipType) {
			if (!forecastObject.has(precipType)) {
				return 0.0;
			}

			JsonObject precipObject = forecastObject.getAsJsonObject(precipType);
			// Intentar primero con datos de 1 hora, luego con 3 horas
			if (precipObject.has("1h")) {
				return precipObject.get("1h").getAsDouble();
			} else if (precipObject.has("3h")) {
				return precipObject.get("3h").getAsDouble();
			}

			return 0.0;
		}

		/**
		 * Método utilitario para extraer valores de objetos JSON anidados con manejo de nulos
		 */
		private <T> T extractJsonValue(JsonObject object, String objectName, String fieldName,
									   Function<JsonElement, T> converter, T defaultValue) {
			if (!object.has(objectName)) {
				return defaultValue;
			}

			JsonObject nestedObject = object.getAsJsonObject(objectName);
			if (!nestedObject.has(fieldName)) {
				return defaultValue;
			}

			return converter.apply(nestedObject.get(fieldName));
		}
	}
}