package org.messiyronaldo;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Repositorio para la persistencia de datos de precios de energía en SQLite
 */
public class EnergyPriceRepository {
	private final String dbPath;

	/**
	 * Constructor con la ruta de la base de datos
	 *
	 * @param dbPath Ruta del archivo de base de datos SQLite
	 */
	public EnergyPriceRepository(String dbPath) {
		this.dbPath = dbPath;
		initializeDatabase();
	}

	/**
	 * Inicializa la base de datos creando las tablas necesarias si no existen
	 */
	private void initializeDatabase() {
		try (Connection conn = getConnection()) {
			// Crear tabla de precios de energía
			try (Statement stmt = conn.createStatement()) {
				stmt.execute(
						"CREATE TABLE IF NOT EXISTS energy_prices (" +
								"id INTEGER PRIMARY KEY AUTOINCREMENT, " +
								"timestamp TEXT NOT NULL, " +
								"price_timestamp TEXT NOT NULL, " +
								"price_pvpc REAL NOT NULL, " +
								"price_spot REAL NOT NULL, " +
								"UNIQUE(price_timestamp)" +
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
	 * Guarda o actualiza una lista de precios de energía
	 * Implementa lógica diferencial para evitar actualizaciones innecesarias
	 *
	 * @param prices Lista de precios a guardar
	 */
	public void saveEnergyPrices(List<EnergyPrice> prices) {
		if (prices == null || prices.isEmpty()) {
			return;
		}

		try (Connection conn = getConnection()) {
			// Desactivar auto-commit para operaciones más rápidas
			conn.setAutoCommit(false);

			// Consultar precios existentes con sus valores
			Map<String, double[]> existingPrices = getExistingPricesWithValues(conn);

			// Preparar sentencias SQL para inserción y actualización
			String insertSql = "INSERT INTO energy_prices " +
					"(timestamp, price_timestamp, price_pvpc, price_spot) " +
					"VALUES (?, ?, ?, ?)";

			String updateSql = "UPDATE energy_prices SET " +
					"timestamp = ?, price_pvpc = ?, price_spot = ? " +
					"WHERE price_timestamp = ?";

			try (PreparedStatement insertStmt = conn.prepareStatement(insertSql);
				 PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {

				int inserts = 0;
				int updates = 0;
				int unchanged = 0;

				// Procesar cada precio
				for (EnergyPrice price : prices) {
					String priceKey = price.getPriceTimestamp().toString();

					if (existingPrices.containsKey(priceKey)) {
						double[] existingValues = existingPrices.get(priceKey);
						double existingPvpc = existingValues[0];
						double existingSpot = existingValues[1];

						// Solo actualizar si alguno de los precios ha cambiado
						boolean pvpcChanged = Math.abs(existingPvpc - price.getPricePVPC()) > 0.0001;
						boolean spotChanged = Math.abs(existingSpot - price.getPriceSpot()) > 0.0001;

						if (pvpcChanged || spotChanged) {
							updateStmt.setString(1, price.getTimestamp().toString());
							updateStmt.setDouble(2, price.getPricePVPC());
							updateStmt.setDouble(3, price.getPriceSpot());
							updateStmt.setString(4, priceKey);
							updateStmt.addBatch();
							updates++;
						} else {
							unchanged++;
						}
					} else {
						// Insertar nuevo precio
						insertStmt.setString(1, price.getTimestamp().toString());
						insertStmt.setString(2, priceKey);
						insertStmt.setDouble(3, price.getPricePVPC());
						insertStmt.setDouble(4, price.getPriceSpot());
						insertStmt.addBatch();
						inserts++;
					}
				}

				// Ejecutar los lotes de instrucciones
				insertStmt.executeBatch();
				updateStmt.executeBatch();

				// Confirmar la transacción
				conn.commit();

				System.out.println("Resumen de operaciones en precios: " +
						inserts + " nuevos, " +
						updates + " actualizados, " +
						unchanged + " sin cambios.");

			}
		} catch (SQLException e) {
			System.err.println("Error al guardar precios: " + e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Recupera precios existentes con sus valores de PVPC y Spot
	 */
	private Map<String, double[]> getExistingPricesWithValues(Connection conn) throws SQLException {
		Map<String, double[]> prices = new HashMap<>();

		String sql = "SELECT price_timestamp, price_pvpc, price_spot FROM energy_prices";
		try (PreparedStatement pstmt = conn.prepareStatement(sql);
			 ResultSet rs = pstmt.executeQuery()) {

			while (rs.next()) {
				String priceTimestamp = rs.getString("price_timestamp");
				double pricePvpc = rs.getDouble("price_pvpc");
				double priceSpot = rs.getDouble("price_spot");

				double[] priceValues = new double[] {pricePvpc, priceSpot};
				prices.put(priceTimestamp, priceValues);
			}
		}

		return prices;
	}

	/**
	 * Obtiene los precios para un rango de fechas
	 *
	 * @param startTime Timestamp inicial
	 * @param endTime Timestamp final
	 * @return Lista de precios que cumplen los criterios
	 */
	public List<EnergyPrice> getEnergyPrices(Instant startTime, Instant endTime) {
		List<EnergyPrice> prices = new ArrayList<>();

		try (Connection conn = getConnection()) {
			String sql =
					"SELECT * FROM energy_prices " +
							"WHERE price_timestamp >= ? AND price_timestamp <= ? " +
							"ORDER BY price_timestamp";

			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setString(1, startTime.toString());
				pstmt.setString(2, endTime.toString());

				try (ResultSet rs = pstmt.executeQuery()) {
					while (rs.next()) {
						EnergyPrice price = new EnergyPrice(
								Instant.parse(rs.getString("timestamp")),
								Instant.parse(rs.getString("price_timestamp")),
								rs.getDouble("price_pvpc"),
								rs.getDouble("price_spot")
						);

						prices.add(price);
					}
				}
			}
		} catch (SQLException e) {
			System.err.println("Error al obtener precios: " + e.getMessage());
			e.printStackTrace();
		}

		return prices;
	}

	/**
	 * Obtiene los precios para un día específico
	 *
	 * @param date Fecha para la que se quieren obtener los precios
	 * @return Lista de precios del día solicitado
	 */
	public List<EnergyPrice> getEnergyPricesByDate(LocalDate date) {
		// Convertir LocalDate a Instant para el inicio y fin del día
		ZoneId zoneId = ZoneId.of("Europe/Madrid");
		Instant startOfDay = date.atStartOfDay(zoneId).toInstant();
		Instant endOfDay = date.plusDays(1).atStartOfDay(zoneId).minusSeconds(1).toInstant();

		return getEnergyPrices(startOfDay, endOfDay);
	}
}