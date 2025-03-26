package org.messiyronaldo.config;

import org.messiyronaldo.energy.control.EnergyPricesProvider;
import org.messiyronaldo.energy.control.REEEnergyProvider;
import org.messiyronaldo.energy.control.EnergyPricesStore;
import org.messiyronaldo.energy.control.SQLiteEnergyPriceStore;
import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.control.OpenWeatherProvider;
import org.messiyronaldo.weather.control.WeatherProvider;
import org.messiyronaldo.weather.control.SQLiteWeatherStore;
import org.messiyronaldo.weather.control.WeatherStore;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Clase de configuración centralizada para el sistema fotovoltaico.
 * Contiene toda la lógica de configuración y creación de componentes.
 */
public class PhotovoltaicSystemConfig {
	// Constantes de configuración
	private static final String DEFAULT_DB_PATH = "photovoltaic-data.db";

	// Intervalos de actualización (en horas)
	private static final long DEFAULT_WEATHER_UPDATE_INTERVAL_HOURS = 6;
	private static final long DEFAULT_ENERGY_UPDATE_INTERVAL_HOURS = 12;

	// Campos de configuración
	private final String dbPath;
	private final String weatherApiKey;
	private final long weatherUpdateIntervalHours;
	private final long energyUpdateIntervalHours;

	/**
	 * Constructor principal que permite especificar toda la configuración.
	 */
	public PhotovoltaicSystemConfig(String weatherApiKey, String dbPath,
									long weatherUpdateIntervalHours, long energyUpdateIntervalHours) {
		this.weatherApiKey = weatherApiKey;
		this.dbPath = dbPath;
		this.weatherUpdateIntervalHours = weatherUpdateIntervalHours;
		this.energyUpdateIntervalHours = energyUpdateIntervalHours;
	}

	/**
	 * Constructor con valores por defecto para los intervalos.
	 */
	public PhotovoltaicSystemConfig(String weatherApiKey, String dbPath) {
		this(weatherApiKey, dbPath, DEFAULT_WEATHER_UPDATE_INTERVAL_HOURS, DEFAULT_ENERGY_UPDATE_INTERVAL_HOURS);
	}

	/**
	 * Constructor con valores por defecto para la ruta de la base de datos.
	 */
	public PhotovoltaicSystemConfig(String weatherApiKey) {
		this(weatherApiKey, createDefaultDbPath());
	}

	/**
	 * Crea la ruta por defecto para la base de datos.
	 */
	private static String createDefaultDbPath() {
		return Paths.get(System.getProperty("user.dir"), DEFAULT_DB_PATH).toString();
	}

	/**
	 * Configura las ubicaciones a monitorizar.
	 */
	public List<Location> configureLocations() {
		List<Location> locations = new ArrayList<>();
		locations.add(new Location("Madrid", 40.4165, -3.7026));
		locations.add(new Location("Las Palmas", 28.151286, -15.427340));
		// Aquí se pueden añadir más ubicaciones según sea necesario
		return locations;
	}

	/**
	 * Configura el proveedor de datos meteorológicos.
	 */
	public WeatherProvider configureWeatherProvider() {
		// Por defecto usamos OpenWeatherProvider, pero podría cambiarse por otro
		return new OpenWeatherProvider(weatherApiKey);

		// Ejemplo para cambiar a otro proveedor:
		// return new AnotherWeatherProvider(otherConfigs);
	}

	/**
	 * Configura el proveedor de precios de energía.
	 */
	public EnergyPricesProvider configureEnergyProvider() {
		// Por defecto usamos REEEnergyProvider, pero podría cambiarse por otro
		return new REEEnergyProvider();

		// Ejemplo para cambiar a otro proveedor:
		// return new AnotherEnergyProvider(otherConfigs);
	}

	/**
	 * Configura el almacén de datos meteorológicos.
	 */
	public WeatherStore configureWeatherStore() {
		// Por defecto usamos SQLite, pero podría cambiarse por otro
		return new SQLiteWeatherStore(dbPath);

		// Ejemplo para cambiar a otro almacén:
		// return new MongoDBWeatherStore(mongoConnectionString);
	}

	/**
	 * Configura el almacén de precios de energía.
	 */
	public EnergyPricesStore configureEnergyStore() {
		// Por defecto usamos SQLite, pero podría cambiarse por otro
		return new SQLiteEnergyPriceStore(dbPath);

		// Ejemplo para cambiar a otro almacén:
		// return new CloudEnergyPriceStore(cloudCredentials);
	}

	/**
	 * Devuelve el intervalo de actualización para datos meteorológicos.
	 */
	public long getWeatherUpdateIntervalHours() {
		return weatherUpdateIntervalHours;
	}

	/**
	 * Devuelve el intervalo de actualización para precios de energía.
	 */
	public long getEnergyUpdateIntervalHours() {
		return energyUpdateIntervalHours;
	}
}