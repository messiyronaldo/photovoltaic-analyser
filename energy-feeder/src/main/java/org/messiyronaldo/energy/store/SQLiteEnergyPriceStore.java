package org.messiyronaldo.energy.store;

import org.messiyronaldo.energy.model.EnergyPrice;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

/**
 * Repositorio para la persistencia de datos de precios de energía en SQLite
 */
public class SQLiteEnergyPriceStore implements EnergyPricesStore {
	private final String dbPath;

	/**
	 * Constructor con la ruta de la base de datos
	 *
	 * @param dbPath Ruta del archivo de base de datos SQLite
	 */
	public SQLiteEnergyPriceStore(String dbPath) {
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
	 * Obtiene un mapa con todos los precios existentes en la BD
	 * @return Mapa donde:
	 *         - Clave: price_timestamp (String)
	 *         - Valor: arreglo con [price_pvpc, price_spot] (double[])
	 */
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

	/**
	 * Guarda o actualiza una lista de precios de energía
	 * Implementa lógica diferencial para evitar actualizaciones innecesarias
	 *
	 * @param prices Lista de precios a guardar
	 */
	public void saveEnergyPrices(List<EnergyPrice> prices) {
		if (prices == null || prices.isEmpty()) {
			System.out.println("Lista de precios vacía - nada que guardar");
			return;
		}

		try (Connection conn = getConnection()) {
			conn.setAutoCommit(false);

			// 1. Obtener todos los registros existentes (timestamp + valores)
			Map<String, double[]> existingPrices = getExistingPriceMap(conn);

			// 2. Preparar sentencias para INSERT y UPDATE
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

				for (EnergyPrice newPrice : prices) {
					String timestampKey = newPrice.getPriceTimestamp().toString();

					if (existingPrices.containsKey(timestampKey)) {
						// REGISTRO EXISTENTE - Verificar si hay cambios
						double[] existingValues = existingPrices.get(timestampKey);
						boolean pvpcChanged = Math.abs(existingValues[0] - newPrice.getPricePVPC()) > 0.0001;
						boolean spotChanged = Math.abs(existingValues[1] - newPrice.getPriceSpot()) > 0.0001;

						if (pvpcChanged || spotChanged) {
							// ACTUALIZAR registro existente
							updateStmt.setString(1, Instant.now().toString());
							updateStmt.setDouble(2, newPrice.getPricePVPC());
							updateStmt.setDouble(3, newPrice.getPriceSpot());
							updateStmt.setString(4, timestampKey);
							updateStmt.addBatch();
							updates++;
						} else {
							unchanged++;
						}
					} else {
						// NUEVO REGISTRO - Insertar
						insertStmt.setString(1, Instant.now().toString());
						insertStmt.setString(2, timestampKey);
						insertStmt.setDouble(3, newPrice.getPricePVPC());
						insertStmt.setDouble(4, newPrice.getPriceSpot());
						insertStmt.addBatch();
						inserts++;
					}
				}

				// Ejecutar ambas operaciones
				insertStmt.executeBatch();
				updateStmt.executeBatch();
				conn.commit();

				System.out.println("=== Resumen Guardado Diferencial ===");
				System.out.println("Nuevos registros insertados: " + inserts);
				System.out.println("Registros actualizados: " + updates);
				System.out.println("Registros sin cambios: " + unchanged);
				System.out.println("Total procesados: " + prices.size());
			}
		} catch (SQLException e) {
			System.err.println("Error al guardar precios: " + e.getMessage());
			e.printStackTrace();
		}
	}

	// Método auxiliar para obtener solo los timestamps existentes
	private Set<String> getExistingTimestamps(Connection conn) throws SQLException {
		Set<String> timestamps = new HashSet<>();
		String sql = "SELECT price_timestamp FROM energy_prices";

		try (PreparedStatement pstmt = conn.prepareStatement(sql);
			 ResultSet rs = pstmt.executeQuery()) {
			while (rs.next()) {
				timestamps.add(rs.getString("price_timestamp"));
			}
		}
		return timestamps;
	}

	// Método auxiliar para comparar precios
	private boolean hasPriceChanged(EnergyPrice lastPrice, EnergyPrice newPrice) {
		return Math.abs(lastPrice.getPricePVPC() - newPrice.getPricePVPC()) > 0.0001 ||
				Math.abs(lastPrice.getPriceSpot() - newPrice.getPriceSpot()) > 0.0001;
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