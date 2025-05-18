package org.messiyronaldo.energy.control;

import org.messiyronaldo.energy.model.EnergyPrice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class SQLiteEnergyPriceStore implements EnergyPricesStore {
	private static final Logger logger = LoggerFactory.getLogger(SQLiteEnergyPriceStore.class);
	private static final ZoneId SPAIN_ZONE_ID = ZoneId.of("Europe/Madrid");
	private static final double PRICE_COMPARISON_TOLERANCE = 0.0001;

	@Override
	public void saveEnergyPrice(EnergyPrice energyPrice) {
		try (Connection conn = getConnection()) {
			String sql = "INSERT INTO energy_prices (ts, price_timestamp, price_pvpc, price_spot, ss) " +
					"VALUES (?, ?, ?, ?, ?)";
			try (PreparedStatement stmt = conn.prepareStatement(sql)) {
				stmt.setString(1, Instant.now().toString());
				stmt.setString(2, energyPrice.getTs().toString());
				stmt.setDouble(3, energyPrice.getPricePVPC());
				stmt.setDouble(4, energyPrice.getPriceSpot());
				stmt.setString(5, energyPrice.getSs());
				stmt.executeUpdate();
			}
		} catch (SQLException e) {
			logger.error("Failed to save energy price: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to save energy price", e);
		}
	}

	private final String dbPath;

	public SQLiteEnergyPriceStore(String dbPath) {
		this.dbPath = dbPath;
		initializeDatabase();
		logger.info("SQLite energy price store initialized with database: {}", dbPath);
	}

	private void initializeDatabase() {
		try (Connection conn = getConnection()) {
			createTables(conn);
			logger.debug("Database tables initialized successfully");
		} catch (SQLException e) {
			logger.error("Failed to initialize database: {}", e.getMessage(), e);
			throw new RuntimeException("Failed to initialize database", e);
		}
	}

	private void createTables(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.execute(createPricesTableSql());
		}
	}

	private String createPricesTableSql() {
		return "CREATE TABLE IF NOT EXISTS energy_prices (" +
				"id INTEGER PRIMARY KEY AUTOINCREMENT, " +
				"ts TEXT NOT NULL, " +
				"price_timestamp TEXT NOT NULL, " +
				"price_pvpc REAL NOT NULL, " +
				"price_spot REAL NOT NULL, " +
				"ss TEXT NOT NULL, " +
				"UNIQUE(price_timestamp)" +
				")";
	}

	private Connection getConnection() throws SQLException {
		try {
			Class.forName("org.sqlite.JDBC");
			return DriverManager.getConnection("jdbc:sqlite:" + dbPath);
		} catch (ClassNotFoundException e) {
			logger.error("SQLite driver not found: {}", e.getMessage(), e);
			throw new SQLException("SQLite driver not found", e);
		}
	}

	private Map<String, double[]> getExistingPriceMap(Connection conn) throws SQLException {
		Map<String, double[]> priceMap = new HashMap<>();
		String sql = "SELECT price_timestamp, price_pvpc, price_spot FROM energy_prices";

		try (PreparedStatement pstmt = conn.prepareStatement(sql);
			 ResultSet rs = pstmt.executeQuery()) {

			while (rs.next()) {
				String timestamp = rs.getString("price_timestamp");
				double pvpc = rs.getDouble("price_pvpc");
				double spot = rs.getDouble("price_spot");

				priceMap.put(timestamp, new double[]{pvpc, spot});
			}
		}

		return priceMap;
	}

	@Override
	public void saveEnergyPrices(List<EnergyPrice> prices) {
		if (isEmpty(prices)) {
			logger.debug("Empty price list - nothing to save");
			return;
		}

		try (Connection conn = getConnection()) {
			processPriceUpdates(conn, prices);
		} catch (SQLException e) {
			logger.error("Failed to save energy prices: {}", e.getMessage(), e);
		}
	}

	private boolean isEmpty(List<EnergyPrice> prices) {
		return prices == null || prices.isEmpty();
	}

	private void processPriceUpdates(Connection conn, List<EnergyPrice> prices) throws SQLException {
		conn.setAutoCommit(false);

		try {
			Map<String, double[]> existingPrices = getExistingPriceMap(conn);
			int[] counts = executeUpdates(conn, prices, existingPrices);

			conn.commit();
			logOperationSummary(counts[0], counts[1], counts[2], prices.size());
		} catch (SQLException e) {
			conn.rollback();
			logger.error("Transaction rolled back due to error: {}", e.getMessage(), e);
			throw e;
		} finally {
			conn.setAutoCommit(true);
		}
	}

	private int[] executeUpdates(Connection conn, List<EnergyPrice> prices,
								 Map<String, double[]> existingPrices) throws SQLException {
		String insertSql = createInsertSql();
		String updateSql = createUpdateSql();

		try (PreparedStatement insertStmt = conn.prepareStatement(insertSql);
			 PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

			return processPriceBatch(insertStmt, updateStmt, prices, existingPrices);
		}
	}

	private String createInsertSql() {
		return "INSERT INTO energy_prices " +
				"(ts, price_timestamp, price_pvpc, price_spot, ss) " +
				"VALUES (?, ?, ?, ?, ?)";
	}

	private String createUpdateSql() {
		return "UPDATE energy_prices SET " +
				"ts = ?, price_pvpc = ?, price_spot = ? " +
				"WHERE price_timestamp = ?";
	}

	private int[] processPriceBatch(PreparedStatement insertStmt, PreparedStatement updateStmt,
									List<EnergyPrice> prices, Map<String, double[]> existingPrices)
			throws SQLException {
		int inserts = 0;
		int updates = 0;
		int unchanged = 0;

		for (EnergyPrice price : prices) {
			String timestampKey = price.getPriceTimestamp().toString();

			if (existingPrices.containsKey(timestampKey)) {
				if (hasPriceChanged(existingPrices.get(timestampKey), price)) {
					addToUpdateBatch(updateStmt, price, timestampKey);
					updates++;
				} else {
					unchanged++;
				}
			} else {
				addToInsertBatch(insertStmt, price, timestampKey);
				inserts++;
			}
		}

		executeNonEmptyBatch(insertStmt, inserts);
		executeNonEmptyBatch(updateStmt, updates);

		return new int[]{inserts, updates, unchanged};
	}

	private void executeNonEmptyBatch(PreparedStatement stmt, int count) throws SQLException {
		if (count > 0) {
			stmt.executeBatch();
		}
	}

	private boolean hasPriceChanged(double[] existingValues, EnergyPrice newPrice) {
		boolean pvpcChanged = Math.abs(existingValues[0] - newPrice.getPricePVPC()) > PRICE_COMPARISON_TOLERANCE;
		boolean spotChanged = Math.abs(existingValues[1] - newPrice.getPriceSpot()) > PRICE_COMPARISON_TOLERANCE;

		return pvpcChanged || spotChanged;
	}

	private void addToInsertBatch(PreparedStatement stmt, EnergyPrice price, String timestampKey)
			throws SQLException {
		stmt.setString(1, Instant.now().toString());
		stmt.setString(2, timestampKey);
		stmt.setDouble(3, price.getPricePVPC());
		stmt.setDouble(4, price.getPriceSpot());
		stmt.setString(5, price.getSs());
		stmt.addBatch();
	}

	private void addToUpdateBatch(PreparedStatement stmt, EnergyPrice price, String timestampKey)
			throws SQLException {
		stmt.setString(1, Instant.now().toString());
		stmt.setDouble(2, price.getPricePVPC());
		stmt.setDouble(3, price.getPriceSpot());
		stmt.setString(4, timestampKey);
		stmt.addBatch();
	}

	private void logOperationSummary(int inserts, int updates, int unchanged, int total) {
		logger.info("Energy price operations summary - New: {}, Updated: {}, Unchanged: {}, Total: {}",
			inserts, updates, unchanged, total);
	}

	@Override
	public List<EnergyPrice> getEnergyPrices(Instant startTime, Instant endTime) {
		List<EnergyPrice> prices = new ArrayList<>();

		try (Connection conn = getConnection()) {
			prices = queryPricesByTimeRange(conn, startTime, endTime);
			logger.debug("Retrieved {} energy prices for time range: {} to {}",
				prices.size(), startTime, endTime);
		} catch (SQLException e) {
			logger.error("Failed to retrieve energy prices: {}", e.getMessage(), e);
		}

		return prices;
	}

	private List<EnergyPrice> queryPricesByTimeRange(Connection conn, Instant startTime, Instant endTime)
			throws SQLException {
		List<EnergyPrice> prices = new ArrayList<>();
		String sql = "SELECT * FROM energy_prices " +
				"WHERE price_timestamp >= ? AND price_timestamp <= ? " +
				"ORDER BY price_timestamp";

		try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
			pstmt.setString(1, startTime.toString());
			pstmt.setString(2, endTime.toString());

			try (ResultSet rs = pstmt.executeQuery()) {
				while (rs.next()) {
					prices.add(createPriceFromResultSet(rs));
				}
			}
		}

		return prices;
	}

	private EnergyPrice createPriceFromResultSet(ResultSet rs) throws SQLException {
		return new EnergyPrice(
				Instant.parse(rs.getString("ts")),
				Instant.parse(rs.getString("price_timestamp")),
				rs.getDouble("price_pvpc"),
				rs.getDouble("price_spot"),
				rs.getString("ss")
		);
	}

	public List<EnergyPrice> getEnergyPricesByDate(LocalDate date) {
		Instant startOfDay = date.atStartOfDay(SPAIN_ZONE_ID).toInstant();
		Instant endOfDay = date.plusDays(1).atStartOfDay(SPAIN_ZONE_ID).minusSeconds(1).toInstant();

		return getEnergyPrices(startOfDay, endOfDay);
	}
}