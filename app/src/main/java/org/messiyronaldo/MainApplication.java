package org.messiyronaldo;

import org.messiyronaldo.config.PhotovoltaicSystemConfig;
import org.messiyronaldo.controller.PhotovoltaicController;
import org.messiyronaldo.energy.control.EnergyPricesProvider;
import org.messiyronaldo.energy.control.EnergyPricesStore;
import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.control.WeatherProvider;
import org.messiyronaldo.weather.control.WeatherStore;

import java.util.List;

/**
 * Aplicación principal para el sistema de análisis fotovoltaico.
 * Utiliza la configuración centralizada para crear el controlador con
 * los proveedores y almacenes específicos.
 */
public class MainApplication {

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Error: Debe proporcionar la API KEY de OpenWeather como argumento.");
			System.err.println("Uso: java -jar app.jar <OPENWEATHER_API_KEY>");
			System.exit(1);
		}

		String apiKey = args[0];

		try {
			System.out.println("Iniciando sistema de análisis fotovoltaico...");

			// Crear la configuración del sistema
			PhotovoltaicSystemConfig config = new PhotovoltaicSystemConfig(apiKey);

			// Obtener todos los componentes necesarios de la configuración
			List<Location> locations = config.configureLocations();
			WeatherProvider weatherProvider = config.configureWeatherProvider();
			EnergyPricesProvider energyProvider = config.configureEnergyProvider();
			WeatherStore weatherStore = config.configureWeatherStore();
			EnergyPricesStore energyStore = config.configureEnergyStore();

			// Crear e iniciar el controlador con las implementaciones configuradas
			PhotovoltaicController controller = new PhotovoltaicController(
					weatherProvider,
					weatherStore,
					energyProvider,
					energyStore,
					locations,
					config.getWeatherUpdateIntervalHours(),
					config.getEnergyUpdateIntervalHours()
			);

			controller.start();

			// Registrar hook de apagado para detener correctamente el controlador
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				System.out.println("Deteniendo sistema...");
				controller.stop();
				System.out.println("Sistema detenido correctamente.");
			}));

			System.out.println("Sistema iniciado correctamente. Presione Ctrl+C para detener.");

			// Mantener la aplicación en ejecución
			Thread.currentThread().join();

		} catch (InterruptedException e) {
			System.err.println("Aplicación interrumpida: " + e.getMessage());
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			System.err.println("Error inesperado: " + e.getMessage());
			e.printStackTrace();
		}
	}
}