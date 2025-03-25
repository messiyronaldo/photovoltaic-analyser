package org.messiyronaldo;

import org.messiyronaldo.energy.model.EnergyPrice;
import org.messiyronaldo.energy.provider.REEEnergyProvider;
import org.messiyronaldo.energy.store.SQLiteEnergyPriceStore;
import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;
import org.messiyronaldo.weather.provider.OpenWeatherProvider;
import org.messiyronaldo.weather.store.SQLiteWeatherStore;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Aplicación principal para el análisis fotovoltaico
 * Combina datos meteorológicos y precios de energía
 */
public class App {
	private static final String DB_PATH = "photovoltaic-data.db";

	// Ubicaciones predefinidas para facilitar pruebas
	private static final Location MADRID = new Location("Madrid", 40.4165, -3.7026);
	private static final Location LASPALMAS = new Location("Las Palmas", 28.151286, -15.427340);

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Error: Debe proporcionar la API KEY de OpenWeather como argumento.");
			System.err.println("Uso: java -jar app.jar <OPENWEATHER_API_KEY> [FECHA]");
			System.err.println("Si no se proporciona fecha, se usará la fecha actual.");
			System.exit(1);
		}

		String apiKey = args[0];
		String dbPath = Paths.get(System.getProperty("user.dir"), DB_PATH).toString();

		// Determinar la fecha a consultar (actual o proporcionada)
		LocalDate dateToQuery = LocalDate.now();
		if (args.length > 1) {
			try {
				dateToQuery = LocalDate.parse(args[1], DateTimeFormatter.ISO_LOCAL_DATE);
			} catch (Exception e) {
				System.err.println("Error: El formato de fecha debe ser YYYY-MM-DD.");
				System.exit(1);
			}
		}

		System.out.println("Fecha a consultar: " + dateToQuery);

		try {
			// Obtener datos meteorológicos
			fetchWeatherData(apiKey, dbPath);

			// Obtener datos de precios de energía
			fetchEnergyPriceData(dbPath, dateToQuery);

			System.out.println("\nOperación completada con éxito.");

		} catch (IOException e) {
			System.err.println("Error en la comunicación con APIs: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("Error inesperado: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Obtiene y guarda datos meteorológicos
	 */
	private static void fetchWeatherData(String apiKey, String dbPath) throws IOException {
		// Inicializar componentes para clima
		OpenWeatherProvider weatherProvider = new OpenWeatherProvider(apiKey);
		SQLiteWeatherStore weatherRepository = new SQLiteWeatherStore(dbPath);

		// Preparar lista de ubicaciones
		List<Location> locations = new ArrayList<>();
		locations.add(MADRID);
		locations.add(LASPALMAS);

		System.out.println("===== DATOS METEOROLÓGICOS =====");

		// Procesar cada ubicación
		for (Location location : locations) {
			System.out.println("Obteniendo pronóstico para " + location.getName() + "...");
			List<Weather> forecast = weatherProvider.getHourlyForecast(location);

			System.out.println("Guardando " + forecast.size() + " pronósticos para " + location.getName());
			weatherRepository.saveWeatherForecasts(forecast);

			// Mostrar datos guardados
			List<Weather> savedForecasts = weatherRepository.getWeatherForecasts(
					location.getLatitude(), location.getLongitude());
			System.out.println("Pronósticos guardados: " + savedForecasts.size());

			// Mostrar ejemplo de datos meteorológicos
			if (!savedForecasts.isEmpty()) {
				System.out.println("\nEjemplo de pronóstico guardado para " + location.getName() + ":");
				Weather example = savedForecasts.get(0);
				System.out.println("Fecha de predicción: " + example.getPredictionTimestamp());
				System.out.println("Temperatura: " + example.getTemperature() + "°C");
				System.out.println("Condición: " + example.getWeatherDescription());
				System.out.println("Humedad: " + example.getHumidity() + "%");
				System.out.println("Viento: " + example.getWindSpeed() + " m/s");
				System.out.println("-------------------------");
			}
		}
	}

	/**
	 * Obtiene y guarda datos de precios de energía para una fecha específica
	 */
	private static void fetchEnergyPriceData(String dbPath, LocalDate date) throws IOException {
		// Inicializar componentes para precios de energía
		REEEnergyProvider energyProvider = new REEEnergyProvider();
		SQLiteEnergyPriceStore energyRepository = new SQLiteEnergyPriceStore(dbPath);

		System.out.println("\n===== PRECIOS DE ENERGÍA =====");
		System.out.println("Obteniendo precios de energía para: " + date);

		// Obtener precios para la fecha especificada
		List<EnergyPrice> prices = energyProvider.getEnergyPrices(date);

		// Guardar precios en la base de datos
		System.out.println("Guardando " + prices.size() + " precios de energía");
		energyRepository.saveEnergyPrices(prices);

		// Verificar precios guardados
		List<EnergyPrice> savedPrices = energyRepository.getEnergyPricesByDate(date);
		System.out.println("Precios guardados: " + savedPrices.size());

		// Mostrar ejemplos de datos de precios
		if (!savedPrices.isEmpty()) {
			System.out.println("\nEjemplo de precio de energía:");
			printEnergyPriceExample(savedPrices.get(0));
		}
	}

	/**
	 * Imprime un ejemplo de precio de energía
	 */
	private static void printEnergyPriceExample(EnergyPrice price) {
		System.out.println("Timestamp: " + price.getPriceTimestamp());
		System.out.println("Precio Mercado PVPC: " + price.getPricePVPC() + " €/MWh");
		System.out.println("Precio Mercado Spot: " + price.getPriceSpot() + " €/MWh");
		System.out.println("-------------------------");
	}
}