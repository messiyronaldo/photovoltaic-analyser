package org.messiyronaldo;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * Repositorio para la persistencia de datos meteorológicos en SQLite
 */
public class WeatherRepository {
	private final String dbPath;

	/**
	 * Constructor con la ruta de la base de datos
	 *
	 * @param dbPath Ruta del archivo de base de datos SQLite
	 */
	public WeatherRepository(String dbPath) {
		this.dbPath = dbPath;
		initializeDatabase();
	}

	/**
	 * Inicializa la base de datos creando las tablas necesarias si no existen
	 */
	private void initializeDatabase() {
		try (Connection conn = getConnection()) {
			// Crear tabla única de pronósticos con ubicación integrada
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(
						"CREATE TABLE IF NOT EXISTS weather_forecasts (" +
								"id INTEGER PRIMARY KEY AUTOINCREMENT, " +
								"timestamp TEXT NOT NULL, " +
								"prediction_timestamp TEXT NOT NULL, " +
								"location_name TEXT NOT NULL, " +
								"latitude REAL NOT NULL, " +
								"longitude REAL NOT NULL, " +
								"temperature REAL NOT NULL, " +
								"humidity INTEGER NOT NULL, " +
								"weather_id INTEGER NOT NULL, " +
								"weather_main TEXT NOT NULL, " +
								"weather_description TEXT NOT NULL, " +
								"cloudiness INTEGER NOT NULL, " +
								"wind_speed REAL NOT NULL, " +
								"rain_volume REAL NOT NULL, " +
								"snow_volume REAL NOT NULL, " +
								"part_of_day TEXT NOT NULL, " +
								"UNIQUE(latitude, longitude, prediction_timestamp)" +
								")"
				);
			}
		} catch (SQLException e) {
			System.err.println("Error al inicializar la base de datos: " + e.getMessage());
			throw new RuntimeException("Error al inicializar la base de datos", e);
		}
	}

	/**
	 * Obtiene una conexión a la base de datos
	 *
	 * @return Conexión a la base de datos SQLite
	 * @throws SQLException Si hay un error al conectar
	 */
	private Connection getConnection() throws SQLException {
		try {
			// Cargar el driver de SQLite
			Class.forName("org.sqlite.JDBC");
			// Crear conexión
			return DriverManager.getConnection("jdbc:sqlite:" + dbPath);
		} catch (ClassNotFoundException e) {
			throw new SQLException("Driver de SQLite no encontrado", e);
		}
	}

	/**
	 * Guarda o actualiza una lista de pronósticos del tiempo
	 *
	 * @param weatherList Lista de pronósticos a guardar
	 */
	public void saveWeatherForecasts(List<Weather> weatherList) {
		if (weatherList == null || weatherList.isEmpty()) {
			return;
		}

		try (Connection conn = getConnection()) {
			// Desactivar auto-commit para operaciones más rápidas
			conn.setAutoCommit(false);

			// Consultar pronósticos existentes para estas ubicaciones/timestamps
			Map<String, Weather> existingForecasts = getExistingForecasts(conn, weatherList);

			// Preparar sentencias SQL para inserción y actualización
			String insertSql = "INSERT INTO weather_forecasts " +
					"(timestamp, prediction_timestamp, location_name, latitude, longitude, " +
					"temperature, humidity, weather_id, weather_main, weather_description, " +
					"cloudiness, wind_speed, rain_volume, snow_volume, part_of_day) " +
					"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

			String updateSql = "UPDATE weather_forecasts SET " +
					"timestamp = ?, temperature = ?, humidity = ?, " +
					"weather_id = ?, weather_main = ?, weather_description = ?, " +
					"cloudiness = ?, wind_speed = ?, rain_volume = ?, " +
					"snow_volume = ?, part_of_day = ? " +
					"WHERE latitude = ? AND longitude = ? AND prediction_timestamp = ?";

			try (PreparedStatement insertStmt = conn.prepareStatement(insertSql);
				 PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

				// Procesar cada pronóstico
				for (Weather weather : weatherList) {
					Location location = weather.getLocation();
					String predictionKey = location.getLatitude() + "|" +
							location.getLongitude() + "|" +
							weather.getPredictionTimestamp().toString();

					if (existingForecasts.containsKey(predictionKey)) {
						// Actualizar pronóstico existente
						updateStmt.setString(1, weather.getTimestamp().toString());
						updateStmt.setDouble(2, weather.getTemperature());
						updateStmt.setInt(3, weather.getHumidity());
						updateStmt.setInt(4, weather.getWeatherID());
						updateStmt.setString(5, weather.getWeatherMain());
						updateStmt.setString(6, weather.getWeatherDescription());
						updateStmt.setInt(7, weather.getCloudiness());
						updateStmt.setDouble(8, weather.getWindSpeed());
						updateStmt.setDouble(9, weather.getRainVolume());
						updateStmt.setDouble(10, weather.getSnowVolume());
						updateStmt.setString(11, weather.getPartOfDay());
						updateStmt.setDouble(12, location.getLatitude());
						updateStmt.setDouble(13, location.getLongitude());
						updateStmt.setString(14, weather.getPredictionTimestamp().toString());
						updateStmt.addBatch();
					} else {
						// Insertar nuevo pronóstico
						insertStmt.setString(1, weather.getTimestamp().toString());
						insertStmt.setString(2, weather.getPredictionTimestamp().toString());
						insertStmt.setString(3, location.getName());
						insertStmt.setDouble(4, location.getLatitude());
						insertStmt.setDouble(5, location.getLongitude());
						insertStmt.setDouble(6, weather.getTemperature());
						insertStmt.setInt(7, weather.getHumidity());
						insertStmt.setInt(8, weather.getWeatherID());
						insertStmt.setString(9, weather.getWeatherMain());
						insertStmt.setString(10, weather.getWeatherDescription());
						insertStmt.setInt(11, weather.getCloudiness());
						insertStmt.setDouble(12, weather.getWindSpeed());
						insertStmt.setDouble(13, weather.getRainVolume());
						insertStmt.setDouble(14, weather.getSnowVolume());
						insertStmt.setString(15, weather.getPartOfDay());
						insertStmt.addBatch();
					}
				}

				// Ejecutar los lotes de instrucciones
				insertStmt.executeBatch();
				updateStmt.executeBatch();
			}

			// Confirmar la transacción
			conn.commit();

		} catch (SQLException e) {
			System.err.println("Error al guardar pronósticos: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Obtiene los pronósticos existentes para una ubicación
	 *
	 * @param conn Conexión a la base de datos
	 * @param weatherList Lista de pronósticos a consultar
	 * @return Mapa de pronósticos indexados por timestamp de predicción
	 * @throws SQLException Si hay un error al consultar
	 */
	private Map<String, Weather> getExistingForecasts(Connection conn, List<Weather> weatherList) throws SQLException {
		Map<String, Weather> forecasts = new HashMap<>();

		// Preparamos un conjunto de latitudes y longitudes únicas para consultar
		Set<String> locationKeys = new HashSet<>();
		for (Weather weather : weatherList) {
			Location location = weather.getLocation();
			locationKeys.add(location.getLatitude() + "|" + location.getLongitude());
		}

		// Construimos la consulta con un IN para todas las ubicaciones
		StringBuilder placeholders = new StringBuilder();
		List<Object> params = new ArrayList<>();

		for (String locationKey : locationKeys) {
			String[] parts = locationKey.split("\\|");
			double latitude = Double.parseDouble(parts[0]);
			double longitude = Double.parseDouble(parts[1]);

			if (placeholders.length() > 0) placeholders.append(" OR ");
			placeholders.append("(latitude = ? AND longitude = ?)");
			params.add(latitude);
			params.add(longitude);
		}

		String sql = "SELECT latitude, longitude, prediction_timestamp FROM weather_forecasts WHERE " + placeholders;

		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			// Configurar parámetros
			for (int i = 0; i < params.size(); i++) {
				pstmt.setObject(i + 1, params.get(i));
			}

			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next()) {
					double latitude = rs.getDouble("latitude");
					double longitude = rs.getDouble("longitude");
					String predictionTimestamp = rs.getString("prediction_timestamp");

					String key = latitude + "|" + longitude + "|" + predictionTimestamp;
					forecasts.put(key, null); // Solo necesitamos las claves
				}
			}
		}

		return forecasts;
	}

	/**
	 * Obtiene todos los pronósticos para una ubicación
	 *
	 * @param latitude Latitud de la ubicación
	 * @param longitude Longitud de la ubicación
	 * @return Lista de pronósticos ordenados por timestamp de predicción
	 */
	public List<Weather> getWeatherForecasts(double latitude, double longitude) {
		List<Weather> forecasts = new ArrayList<>();

		try (Connection conn = getConnection()) {
			String sql =
					"SELECT * FROM weather_forecasts " +
							"WHERE latitude = ? AND longitude = ? " +
							"ORDER BY prediction_timestamp";

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setDouble(1, latitude);
				pstmt.setDouble(2, longitude);

				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						Location location = new Location(
								rs.getString("location_name"),
								rs.getDouble("latitude"),
								rs.getDouble("longitude")
						);

						Weather weather = new Weather(
								Instant.parse(rs.getString("timestamp")),
								location,
								Instant.parse(rs.getString("prediction_timestamp")),
								rs.getDouble("temperature"),
								rs.getInt("humidity"),
								rs.getInt("weather_id"),
								rs.getString("weather_main"),
								rs.getString("weather_description"),
								rs.getInt("cloudiness"),
								rs.getDouble("wind_speed"),
								rs.getDouble("rain_volume"),
								rs.getDouble("snow_volume"),
								rs.getString("part_of_day")
						);

						forecasts.add(weather);
					}
				}
			}
		} catch (SQLException e) {
			System.err.println("Error al obtener pronósticos: " + e.getMessage());
			e.printStackTrace();
		}

		return forecasts;
	}
}