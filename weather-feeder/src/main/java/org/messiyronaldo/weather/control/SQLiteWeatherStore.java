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
	private static final Logger logger = Logger.getLogger(SQLiteWeatherStore.class.getName());

	public SQLiteWeatherStore(String databaseFilePath) {
		this.databaseFilePath = databaseFilePath;
		initializeDatabase();
	}

	private void initializeDatabase() {
		try (Connection connection = openConnection()) {
			createTables(connection);
		} catch (SQLException e) {
			logError("Database initialization failed", e);
			throw new RuntimeException("Failed to initialize database", e);
		}
	}

	private void createTables(Connection connection) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			statement.execute(createWeatherTableSql());
		}
	}

	private String createWeatherTableSql() {
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

	private Connection openConnection() throws SQLException {
		try {
			Class.forName("org.sqlite.JDBC");
			return DriverManager.getConnection("jdbc:sqlite:" + databaseFilePath);
		} catch (ClassNotFoundException e) {
			throw new SQLException("SQLite driver not found", e);
		}
	}

	@Override
	public void saveWeatherForecasts(List<Weather> forecasts) {
		if (isEmpty(forecasts)) return;

		try (Connection connection = openConnection()) {
			executeTransaction(connection, forecasts);
		} catch (SQLException e) {
			logError("Error saving weather forecasts", e);
		}
	}

	private boolean isEmpty(List<Weather> forecasts) {
		return forecasts == null || forecasts.isEmpty();
	}

	private void executeTransaction(Connection connection, List<Weather> forecasts) throws SQLException {
		connection.setAutoCommit(false);

		try {
			Map<String, Weather> existingForecasts = findExistingForecasts(connection, forecasts);
			int[] counts = processForecasts(connection, forecasts, existingForecasts);

			connection.commit();
			logOperationSummary(counts[0], counts[1], counts[2]);
		} catch (SQLException e) {
			rollbackTransaction(connection);
			throw e;
		} finally {
			connection.setAutoCommit(true);
		}
	}

	private void rollbackTransaction(Connection connection) {
		try {
			connection.rollback();
		} catch (SQLException e) {
			logError("Error rolling back transaction", e);
		}
	}

	private int[] processForecasts(Connection connection, List<Weather> forecasts,
								   Map<String, Weather> existingForecasts) throws SQLException {
		String insertSql = createInsertSql();
		String updateSql = createUpdateSql();

		try (PreparedStatement insertStatement = connection.prepareStatement(insertSql);
			 PreparedStatement updateStatement = connection.prepareStatement(updateSql)) {

			return processForecastBatch(insertStatement, updateStatement, forecasts, existingForecasts);
		}
	}

	private String createInsertSql() {
		return "INSERT INTO weather_forecasts " +
				"(timestamp, prediction_timestamp, location_name, latitude, longitude, " +
				"temperature, humidity, weather_id, weather_main, weather_description, " +
				"cloudiness, wind_speed, rain_volume, snow_volume, part_of_day, source_system) " +
				"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	}

	private String createUpdateSql() {
		return "UPDATE weather_forecasts SET " +
				"timestamp = ?, temperature = ?, humidity = ?, " +
				"weather_id = ?, weather_main = ?, weather_description = ?, " +
				"cloudiness = ?, wind_speed = ?, rain_volume = ?, " +
				"snow_volume = ?, part_of_day = ?, source_system = ? " +
				"WHERE latitude = ? AND longitude = ? AND prediction_timestamp = ?";
	}

	private int[] processForecastBatch(PreparedStatement insertStatement, PreparedStatement updateStatement,
									   List<Weather> forecasts, Map<String, Weather> existingForecasts)
			throws SQLException {
		int insertCount = 0;
		int updateCount = 0;
		int unchangedCount = 0;

		for (Weather forecast : forecasts) {
			String key = createForecastKey(forecast);

			if (existingForecasts.containsKey(key)) {
				Weather existingForecast = existingForecasts.get(key);

				if (hasForecastChanged(existingForecast, forecast)) {
					addToUpdateBatch(updateStatement, forecast);
					updateCount++;
				} else {
					unchangedCount++;
				}
			} else {
				addToInsertBatch(insertStatement, forecast);
				insertCount++;
			}
		}

		executeNonEmptyBatch(insertStatement, insertCount);
		executeNonEmptyBatch(updateStatement, updateCount);

		return new int[]{insertCount, updateCount, unchangedCount};
	}

	private void executeNonEmptyBatch(PreparedStatement statement, int count) throws SQLException {
		if (count > 0) {
			statement.executeBatch();
		}
	}

	private String createForecastKey(Weather forecast) {
		Location location = forecast.getLocation();
		return location.getLatitude() + "|" +
				location.getLongitude() + "|" +
				forecast.getPredictionTimestamp().toString();
	}

	private void addToInsertBatch(PreparedStatement statement, Weather forecast) throws SQLException {
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

	private void addToUpdateBatch(PreparedStatement statement, Weather forecast) throws SQLException {
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
		statement.setString(12, forecast.getSourceSystem());
		statement.setDouble(13, location.getLatitude());
		statement.setDouble(14, location.getLongitude());
		statement.setString(15, forecast.getPredictionTimestamp().toString());

		statement.addBatch();
	}

	private boolean hasForecastChanged(Weather existing, Weather newForecast) {
		if (existing.getTemperature() != newForecast.getTemperature()) return true;
		if (existing.getHumidity() != newForecast.getHumidity()) return true;
		if (existing.getWeatherID() != newForecast.getWeatherID()) return true;
		if (existing.getWindSpeed() != newForecast.getWindSpeed()) return true;
		if (existing.getRainVolume() != newForecast.getRainVolume()) return true;
		if (existing.getSnowVolume() != newForecast.getSnowVolume()) return true;
		if (existing.getCloudiness() != newForecast.getCloudiness()) return true;

		return !existing.getWeatherMain().equals(newForecast.getWeatherMain())
				|| !existing.getWeatherDescription().equals(newForecast.getWeatherDescription())
				|| !existing.getPartOfDay().equals(newForecast.getPartOfDay());
	}

	private Map<String, Weather> findExistingForecasts(Connection connection, List<Weather> forecasts)
			throws SQLException {
		Set<LocationCoordinates> uniqueLocations = extractUniqueLocations(forecasts);
		String sql = buildExistingForecastQuery(uniqueLocations);
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

	private String buildExistingForecastQuery(Set<LocationCoordinates> uniqueLocations) {
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

		try (Connection connection = openConnection()) {
			forecasts = queryForecastsByLocation(connection, latitude, longitude);
		} catch (SQLException e) {
			logError("Error retrieving weather forecasts", e);
		}

		return forecasts;
	}

	private List<Weather> queryForecastsByLocation(Connection connection, double latitude, double longitude)
			throws SQLException {
		List<Weather> forecasts = new ArrayList<>();
		String sql = "SELECT * FROM weather_forecasts WHERE latitude = ? AND longitude = ? ORDER BY prediction_timestamp";

		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setDouble(1, latitude);
			statement.setDouble(2, longitude);

			try (ResultSet results = statement.executeQuery()) {
				while (results.next()) {
					forecasts.add(createWeatherFromResultSet(results));
				}
			}
		}

		return forecasts;
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

	private void logOperationSummary(int inserted, int updated, int unchanged) {
		System.out.println("Forecast operations summary: " +
				inserted + " new, " +
				updated + " updated, " +
				unchanged + " unchanged.");
	}

	private void logError(String message, Exception e) {
		logger.log(Level.SEVERE, message, e);
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