package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;

import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLiteWeatherStore implements WeatherStore {
	private final String databaseFilePath;

	public SQLiteWeatherStore(String databaseFilePath) {
		this.databaseFilePath = databaseFilePath;
		createDatabaseTablesIfNotExist();
	}

	private void createDatabaseTablesIfNotExist() {
		try (Connection connection = openDatabaseConnection()) {
			createWeatherForecastTable(connection);
		} catch (SQLException exception) {
			logDatabaseError("Inicialización de la base de datos fallida", exception);
			throw new RuntimeException("Fallo al inicializar la base de datos", exception);
		}
	}

	private void createWeatherForecastTable(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(buildCreateWeatherTableSql());
		}
	}

	private String buildCreateWeatherTableSql() {
		return "CREATE TABLE IF NOT EXISTS weather_forecasts (" +
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
				"source_system TEXT NOT NULL," +
				"UNIQUE(latitude, longitude, prediction_timestamp)" +
				")";
	}

	private Connection openDatabaseConnection() throws SQLException {
		try {
			Class.forName("org.sqlite.JDBC");
			return DriverManager.getConnection("jdbc:sqlite:" + databaseFilePath);
		} catch (ClassNotFoundException exception) {
			throw new SQLException("Driver de SQLite no encontrado", exception);
		}
	}

	@Override
	public void saveWeatherForecasts(List<Weather> forecasts) {
		if (isEmptyForecastList(forecasts)) {
			return;
		}

		try (Connection connection = openDatabaseConnection()) {
			executeDatabaseTransaction(connection, forecasts);
		} catch (SQLException exception) {
			logDatabaseError("Error al guardar pronósticos del clima", exception);
		}
	}

	private boolean isEmptyForecastList(List<Weather> forecasts) {
		return forecasts == null || forecasts.isEmpty();
	}

	private void executeDatabaseTransaction(Connection connection, List<Weather> forecasts)
			throws SQLException {
		disableAutoCommit(connection);

		try {
			Map<String, Weather> existingForecasts = findExistingForecasts(connection, forecasts);
			int[] operationCounts = processForecastBatch(connection, forecasts, existingForecasts);

			connection.commit();
			logOperationSummary(operationCounts[0], operationCounts[1], operationCounts[2]);
		} catch (SQLException exception) {
			rollbackTransaction(connection);
			throw exception;
		} finally {
			enableAutoCommit(connection);
		}
	}

	private void disableAutoCommit(Connection connection) throws SQLException {
		connection.setAutoCommit(false);
	}

	private void enableAutoCommit(Connection connection) throws SQLException {
		connection.setAutoCommit(true);
	}

	private void rollbackTransaction(Connection connection) {
		try {
			connection.rollback();
		} catch (SQLException exception) {
			logDatabaseError("Error al hacer rollback de la transacción", exception);
		}
	}

	private int[] processForecastBatch(Connection connection, List<Weather> forecasts,
									   Map<String, Weather> existingForecasts) throws SQLException {
		String insertSql = buildInsertForecastSql();
		String updateSql = buildUpdateForecastSql();

		try (PreparedStatement insertStatement = connection.prepareStatement(insertSql);
			 PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {

			return addForecastsToBatch(insertStatement, updateStatement, forecasts, existingForecasts);
		}
	}

	private String buildInsertForecastSql() {
		return "INSERT INTO weather_forecasts " +
				"(timestamp, prediction_timestamp, location_name, latitude, longitude, " +
				"temperature, humidity, weather_id, weather_main, weather_description, " +
				"cloudiness, wind_speed, rain_volume, snow_volume, part_of_day, source_system) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	}

	private String buildUpdateForecastSql() {
		return "UPDATE weather_forecasts SET " +
				"timestamp = ?, temperature = ?, humidity = ?, " +
				"weather_id = ?, weather_main = ?, weather_description = ?, " +
				"cloudiness = ?, wind_speed = ?, rain_volume = ?, " +
				"snow_volume = ?, part_of_day = ?, source_system = ? " +
				"WHERE latitude = ? AND longitude = ? AND prediction_timestamp = ?";
	}

	private int[] addForecastsToBatch(PreparedStatement insertStatement, PreparedStatement updateStatement,
									  List<Weather> forecasts, Map<String, Weather> existingForecasts)
			throws SQLException {
		int insertCount = 0;
		int updateCount = 0;
		int unchangedCount = 0;

		for (Weather forecast : forecasts) {
			String forecastKey = createForecastKey(forecast);

			if (existingForecasts.containsKey(forecastKey)) {
				Weather existingForecast = existingForecasts.get(forecastKey);

				if (hasForecastChanged(existingForecast, forecast)) {
					addForecastToUpdateBatch(updateStatement, forecast);
					updateCount++;
				} else {
					unchangedCount++;
				}
			} else {
				addForecastToInsertBatch(insertStatement, forecast);
				insertCount++;
			}
		}

		if (insertCount > 0) {
			insertStatement.executeBatch();
		}

		if (updateCount > 0) {
			updateStatement.executeBatch();
		}

		return new int[]{insertCount, updateCount, unchangedCount};
	}

	private String createForecastKey(Weather forecast) {
		Location location = forecast.getLocation();
		return location.getLatitude() + "|" +
				location.getLongitude() + "|" +
				forecast.getPredictionTimestamp().toString();
	}

	private void addForecastToInsertBatch(PreparedStatement statement, Weather forecast)
			throws SQLException {
		Location location = forecast.getLocation();

		statement.setString(1, forecast.getTimestamp().toString());
		statement.setString(2, forecast.getPredictionTimestamp().toString());
		statement.setString(3, location.getName());
		statement.setDouble(4, location.getLatitude());
		statement.setDouble(5, location.getLongitude());
		statement.setDouble(6, forecast.getTemperature());
		statement.setInt(7, forecast.getHumidity());
		statement.setInt(8, forecast.getWeatherID());
		statement.setString(9, forecast.getWeatherMain());
		statement.setString(10, forecast.getWeatherDescription());
		statement.setInt(11, forecast.getCloudiness());
		statement.setDouble(12, forecast.getWindSpeed());
		statement.setDouble(13, forecast.getRainVolume());
		statement.setDouble(14, forecast.getSnowVolume());
		statement.setString(15, forecast.getPartOfDay());
		statement.setString(16, forecast.getSourceSystem());

		statement.addBatch();
	}

	private void addForecastToUpdateBatch(PreparedStatement statement, Weather forecast)
			throws SQLException {
		Location location = forecast.getLocation();

		statement.setString(1, forecast.getTimestamp().toString());
		statement.setDouble(2, forecast.getTemperature());
		statement.setInt(3, forecast.getHumidity());
		statement.setInt(4, forecast.getWeatherID());
		statement.setString(5, forecast.getWeatherMain());
		statement.setString(6, forecast.getWeatherDescription());
		statement.setInt(7, forecast.getCloudiness());
		statement.setDouble(8, forecast.getWindSpeed());
		statement.setDouble(9, forecast.getRainVolume());
		statement.setDouble(10, forecast.getSnowVolume());
		statement.setString(11, forecast.getPartOfDay());
		statement.setString(12, forecast.getSourceSystem()); // Add this line
		statement.setDouble(13, location.getLatitude());
		statement.setDouble(14, location.getLongitude());
		statement.setString(15, forecast.getPredictionTimestamp().toString());

		statement.addBatch();
	}

	private boolean hasForecastChanged(Weather existingForecast, Weather newForecast) {
		if (existingForecast.getTemperature() != newForecast.getTemperature()) return true;
		if (existingForecast.getHumidity() != newForecast.getHumidity()) return true;
		if (existingForecast.getWeatherID() != newForecast.getWeatherID()) return true;
		if (existingForecast.getWindSpeed() != newForecast.getWindSpeed()) return true;
		if (existingForecast.getRainVolume() != newForecast.getRainVolume()) return true;
		if (existingForecast.getSnowVolume() != newForecast.getSnowVolume()) return true;
		if (existingForecast.getCloudiness() != newForecast.getCloudiness()) return true;

		return !existingForecast.getWeatherMain().equals(newForecast.getWeatherMain())
				|| !existingForecast.getWeatherDescription().equals(newForecast.getWeatherDescription())
				|| !existingForecast.getPartOfDay().equals(newForecast.getPartOfDay());
	}

	private Map<String, Weather> findExistingForecasts(Connection connection, List<Weather> forecasts)
			throws SQLException {
		Set<LocationCoordinates> uniqueLocations = extractUniqueLocations(forecasts);
		String sql = buildExistingForecastsQuery(uniqueLocations);
		List<Object> parameters = createLocationParameters(uniqueLocations);

		return queryExistingForecasts(connection, sql, parameters);
	}

	private Set<LocationCoordinates> extractUniqueLocations(List<Weather> forecasts) {
		Set<LocationCoordinates> uniqueLocations = new HashSet<>();

		for (Weather forecast : forecasts) {
			Location location = forecast.getLocation();
			uniqueLocations.add(new LocationCoordinates(location.getLatitude(), location.getLongitude()));
		}

		return uniqueLocations;
	}

	private String buildExistingForecastsQuery(Set<LocationCoordinates> uniqueLocations) {
		StringBuilder conditions = new StringBuilder();

		for (int i = 0; i < uniqueLocations.size(); i++) {
			if (i > 0) conditions.append(" OR ");
			conditions.append("(latitude = ? AND longitude = ?)");
		}

		return "SELECT * FROM weather_forecasts WHERE " + conditions;
	}

	private List<Object> createLocationParameters(Set<LocationCoordinates> uniqueLocations) {
		List<Object> parameters = new ArrayList<>();

		for (LocationCoordinates location : uniqueLocations) {
			parameters.add(location.latitude);
			parameters.add(location.longitude);
		}

		return parameters;
	}

	private Map<String, Weather> queryExistingForecasts(Connection connection, String sql,
														List<Object> parameters) throws SQLException {
		Map<String, Weather> forecasts = new HashMap<>();

		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			setStatementParameters(statement, parameters);

			try (ResultSet results = statement.executeQuery()) {
				while (results.next()) {
					Weather forecast = createWeatherFromResultSet(results);
					String key = createForecastKey(forecast);
					forecasts.put(key, forecast);
				}
			}
		}

		return forecasts;
	}

	private void setStatementParameters(PreparedStatement statement, List<Object> parameters)
			throws SQLException {
		for (int i = 0; i < parameters.size(); i++) {
			statement.setObject(i + 1, parameters.get(i));
		}
	}

	@Override
	public List<Weather> getWeatherForecasts(double latitude, double longitude) {
		List<Weather> forecasts = new ArrayList<>();

		try (Connection connection = openDatabaseConnection()) {
			String sql = buildForecastsQueryByLocation();

			try (PreparedStatement statement = connection.prepareStatement(sql)) {
				statement.setDouble(1, latitude);
				statement.setDouble(2, longitude);

				try (ResultSet results = statement.executeQuery()) {
					while (results.next()) {
						Weather forecast = createWeatherFromResultSet(results);
						forecasts.add(forecast);
					}
				}
			}
		} catch (SQLException exception) {
			logDatabaseError("Error al obtener los pronósticos del clima", exception);
		}

		return forecasts;
	}

	private String buildForecastsQueryByLocation() {
		return "SELECT * FROM weather_forecasts " +
				"WHERE latitude = ? AND longitude = ? " +
				"ORDER BY prediction_timestamp";
	}

	private Weather createWeatherFromResultSet(ResultSet results) throws SQLException {
		Location location = createLocationFromResultSet(results);

		return new Weather(
				Instant.parse(results.getString("timestamp")),
				location,
				Instant.parse(results.getString("prediction_timestamp")),
				results.getDouble("temperature"),
				results.getInt("humidity"),
				results.getInt("weather_id"),
				results.getString("weather_main"),
				results.getString("weather_description"),
				results.getInt("cloudiness"),
				results.getDouble("wind_speed"),
				results.getDouble("rain_volume"),
				results.getDouble("snow_volume"),
				results.getString("part_of_day"),
				results.getString("source_system")
		);
	}

	private Location createLocationFromResultSet(ResultSet results) throws SQLException {
		return new Location(
				results.getString("location_name"),
				results.getDouble("latitude"),
				results.getDouble("longitude")
		);
	}

	private void logOperationSummary(int insertCount, int updateCount, int unchangedCount) {
		System.out.println("Resumen de operaciones en pronósticos: " +
				insertCount + " nuevos, " +
				updateCount + " actualizados, " +
				unchangedCount + " sin cambios.");
	}

	private void logDatabaseError(String message, Exception exception) {
		Logger logger = Logger.getLogger(SQLiteWeatherStore.class.getName());
		logger.log(Level.SEVERE, message, exception);
	}

	private record LocationCoordinates(double latitude, double longitude) {

		@Override
			public boolean equals(Object o) {
				if (this == o) return true;
				if (o == null || getClass() != o.getClass()) return false;
				LocationCoordinates that = (LocationCoordinates) o;
				return Double.compare(that.latitude, latitude) == 0 &&
						Double.compare(that.longitude, longitude) == 0;
			}

	}
}