package org.messiyronaldo;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

/**
 * Aplicación principal para el análisis fotovoltaico
 */
public class WeatherApp {
	private static final String DB_PATH = "photovoltaic-data.db";

	// Ubicaciones predefinidas para facilitar pruebas
	private static final Location MADRID = new Location("Madrid", 40.4165, -3.7026);
	private static final Location LASPALMAS = new Location("Las Palmas", 28.151286, -15.427340);

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Error: Debe proporcionar la API KEY de OpenWeather como argumento.");
			System.err.println("Uso: java -jar app.jar <OPENWEATHER_API_KEY>");
			System.exit(1);
		}

		String apiKey = args[0];
		String dbPath = Paths.get(System.getProperty("user.dir"), DB_PATH).toString();

		try {
			// Inicializar componentes
			OpenWeatherProvider weatherProvider = new OpenWeatherProvider(apiKey);
			WeatherRepository weatherRepository = new WeatherRepository(dbPath);

			// Obtener datos para Madrid
			System.out.println("Obteniendo pronóstico para " + MADRID.getName() + "...");
			List<Weather> madridForecast = weatherProvider.getHourlyForecast(MADRID);

			// Guardar datos en la base de datos
			System.out.println("Guardando " + madridForecast.size() + " pronósticos para " + MADRID.getName());
			weatherRepository.saveWeatherForecasts(madridForecast);

			// Mostrar datos guardados
			List<Weather> savedForecasts = weatherRepository.getWeatherForecasts(
					MADRID.getLatitude(), MADRID.getLongitude());
			System.out.println("Pronósticos guardados: " + savedForecasts.size());

			// Obtener datos para Las Plamas
			System.out.println("\nObteniendo pronóstico para " + LASPALMAS.getName() + "...");
			List<Weather> barcelonaForecast = weatherProvider.getHourlyForecast(LASPALMAS);

			// Guardar datos en la base de datos
			System.out.println("Guardando " + barcelonaForecast.size() + " pronósticos para " + LASPALMAS.getName());
			weatherRepository.saveWeatherForecasts(barcelonaForecast);

			// Mostrar datos guardados
			savedForecasts = weatherRepository.getWeatherForecasts(
					LASPALMAS.getLatitude(), LASPALMAS.getLongitude());
			System.out.println("Pronósticos guardados: " + savedForecasts.size());

			// Mostrar ejemplo de datos
			if (!savedForecasts.isEmpty()) {
				System.out.println("\nEjemplo de pronóstico guardado:");
				Weather example = savedForecasts.getFirst();
				System.out.println("Ubicación: " + example.getLocation().getName());
				System.out.println("Fecha de predicción: " + example.getPredictionTimestamp());
				System.out.println("Temperatura: " + example.getTemperature() + "°C");
				System.out.println("Condición: " + example.getWeatherDescription());
				System.out.println("Humedad: " + example.getHumidity() + "%");
				System.out.println("Viento: " + example.getWindSpeed() + " m/s");
			}

			System.out.println("\nOperación completada con éxito.");

		} catch (IOException e) {
			System.err.println("Error al consultar la API de OpenWeather: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("Error inesperado: " + e.getMessage());
			e.printStackTrace();
		}
	}
}