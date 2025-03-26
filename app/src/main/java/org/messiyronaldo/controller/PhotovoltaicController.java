package org.messiyronaldo.controller;

import org.messiyronaldo.energy.model.EnergyPrice;
import org.messiyronaldo.energy.provider.EnergyPricesProvider;
import org.messiyronaldo.energy.provider.REEEnergyProvider;
import org.messiyronaldo.energy.store.EnergyPricesStore;
import org.messiyronaldo.energy.store.SQLiteEnergyPriceStore;
import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;
import org.messiyronaldo.weather.provider.OpenWeatherProvider;
import org.messiyronaldo.weather.provider.WeatherProvider;
import org.messiyronaldo.weather.store.SQLiteWeatherStore;
import org.messiyronaldo.weather.store.WeatherStore;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Controlador central para el sistema de análisis fotovoltaico.
 * Gestiona la obtención periódica de datos y su almacenamiento.
 */
public class PhotovoltaicController {
	// Dependencias
	private final WeatherProvider weatherProvider;
	private final WeatherStore weatherStore;
	private final EnergyPricesProvider energyProvider;
	private final EnergyPricesStore energyStore;

	// Configuración
	private final List<Location> locations;
	private final ScheduledExecutorService scheduler;
	private final long weatherUpdateInterval;
	private final long energyUpdateInterval;

	// Bloqueo para sincronizar acceso a la base de datos
	private final ReentrantLock databaseLock = new ReentrantLock();

	/**
	 * Constructor con todas las dependencias.
	 */
	public PhotovoltaicController(
			WeatherProvider weatherProvider,
			WeatherStore weatherStore,
			EnergyPricesProvider energyProvider,
			EnergyPricesStore energyStore,
			List<Location> locations,
			long weatherUpdateIntervalHours,
			long energyUpdateIntervalHours) {

		this.weatherProvider = weatherProvider;
		this.weatherStore = weatherStore;
		this.energyProvider = energyProvider;
		this.energyStore = energyStore;
		this.locations = new ArrayList<>(locations);
		this.weatherUpdateInterval = TimeUnit.HOURS.toMillis(weatherUpdateIntervalHours);
		this.energyUpdateInterval = TimeUnit.HOURS.toMillis(energyUpdateIntervalHours);
		this.scheduler = Executors.newScheduledThreadPool(2);
	}

	/**
	 * Constructor con configuración predeterminada.
	 */
	public static PhotovoltaicController createWithDefaults(String openWeatherApiKey, String dbPath) {
		// Inicializar proveedores
		WeatherProvider weatherProvider = new OpenWeatherProvider(openWeatherApiKey);
		EnergyPricesProvider energyProvider = new REEEnergyProvider();

		// Inicializar almacenes
		WeatherStore weatherStore = new SQLiteWeatherStore(dbPath);
		EnergyPricesStore energyStore = new SQLiteEnergyPriceStore(dbPath);

		// Ubicaciones predefinidas
		List<Location> locations = new ArrayList<>();
		locations.add(new Location("Madrid", 40.4165, -3.7026));
		locations.add(new Location("Las Palmas", 28.151286, -15.427340));

		// Crear controlador con actualizaciones cada 6 horas para clima y 12 horas para precios
		return new PhotovoltaicController(
				weatherProvider, weatherStore, energyProvider, energyStore, locations, 6, 12);
	}

	/**
	 * Inicia las tareas programadas.
	 */
	public void start() {
		// Programar actualización periódica del clima (ejecutar inmediatamente)
		scheduler.scheduleAtFixedRate(
				this::updateWeatherData,
				0,
				weatherUpdateInterval,
				TimeUnit.MILLISECONDS);

		// Programar actualización periódica de precios con un pequeño retraso inicial
		// para evitar colisiones en la base de datos
		scheduler.scheduleAtFixedRate(
				this::updateEnergyPrices,
				5000, // 5 segundos de retraso inicial
				energyUpdateInterval,
				TimeUnit.MILLISECONDS);

		System.out.println("Controlador iniciado con actualizaciones programadas.");
	}

	/**
	 * Detiene todas las tareas programadas.
	 */
	public void stop() {
		scheduler.shutdown();
		try {
			if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
				scheduler.shutdownNow();
			}
		} catch (InterruptedException e) {
			scheduler.shutdownNow();
			Thread.currentThread().interrupt();
		}
		System.out.println("Controlador detenido.");
	}

	/**
	 * Actualiza datos meteorológicos para todas las ubicaciones configuradas.
	 */
	public void updateWeatherData() {
		System.out.println("Iniciando actualización de datos meteorológicos: " +
				LocalDateTime.now());

		try {
			for (Location location : locations) {
				try {
					System.out.println("Obteniendo pronóstico para " + location.getName() + "...");
					List<Weather> forecast = weatherProvider.getHourlyForecast(location);

					System.out.println("Guardando " + forecast.size() + " pronósticos para " +
							location.getName());

					// Adquirir bloqueo antes de acceder a la base de datos
					databaseLock.lock();
					try {
						weatherStore.saveWeatherForecasts(forecast);
					} finally {
						// Asegurarse de liberar el bloqueo incluso si hay excepciones
						databaseLock.unlock();
					}

					// Obtener y mostrar algunos datos para verificación
					databaseLock.lock();
					try {
						List<Weather> savedForecasts = weatherStore.getWeatherForecasts(
								location.getLatitude(), location.getLongitude());
						System.out.println("Pronósticos almacenados para " + location.getName() +
								": " + savedForecasts.size());
					} finally {
						databaseLock.unlock();
					}

				} catch (IOException e) {
					System.err.println("Error al actualizar datos meteorológicos para " +
							location.getName() + ": " + e.getMessage());
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			System.err.println("Error inesperado en actualización meteorológica: " + e.getMessage());
			e.printStackTrace();
		}

		System.out.println("Actualización de datos meteorológicos completada.");
	}

	/**
	 * Actualiza precios de energía para la fecha actual.
	 */
	public void updateEnergyPrices() {
		LocalDate today = LocalDate.now();
		System.out.println("Iniciando actualización de precios de energía para " +
				today + ": " + LocalDateTime.now());

		try {
			// Obtener precios para la fecha actual
			List<EnergyPrice> prices = energyProvider.getEnergyPrices(today);

			// Guardar precios en la base de datos con bloqueo
			System.out.println("Guardando " + prices.size() + " precios de energía");

			databaseLock.lock();
			try {
				energyStore.saveEnergyPrices(prices);
			} finally {
				databaseLock.unlock();
			}

			// Verificar precios guardados con bloqueo
			databaseLock.lock();
			try {
				ZoneId zoneId = ZoneId.of("Europe/Madrid");
				List<EnergyPrice> savedPrices = energyStore.getEnergyPrices(
						today.atStartOfDay(zoneId).toInstant(),
						today.plusDays(1).atStartOfDay(zoneId).minusSeconds(1).toInstant()
				);

				System.out.println("Precios de energía almacenados: " + savedPrices.size());
			} finally {
				databaseLock.unlock();
			}

		} catch (IOException e) {
			System.err.println("Error al actualizar precios de energía: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			System.err.println("Error inesperado en actualización de precios: " + e.getMessage());
			e.printStackTrace();
		}

		System.out.println("Actualización de precios de energía completada.");
	}

	/**
	 * Obtiene los pronósticos meteorológicos más recientes para una ubicación.
	 */
	public List<Weather> getLatestWeatherForecasts(Location location) {
		databaseLock.lock();
		try {
			return weatherStore.getWeatherForecasts(location.getLatitude(), location.getLongitude());
		} finally {
			databaseLock.unlock();
		}
	}

	/**
	 * Obtiene los precios de energía para una fecha específica.
	 */
	public List<EnergyPrice> getEnergyPricesForDate(LocalDate date) {
		databaseLock.lock();
		try {
			ZoneId zoneId = ZoneId.of("Europe/Madrid");
			return energyStore.getEnergyPrices(
					date.atStartOfDay(zoneId).toInstant(),
					date.plusDays(1).atStartOfDay(zoneId).minusSeconds(1).toInstant()
			);
		} finally {
			databaseLock.unlock();
		}
	}
}