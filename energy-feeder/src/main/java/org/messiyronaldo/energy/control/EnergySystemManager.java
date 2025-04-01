package org.messiyronaldo.energy.control;

import org.messiyronaldo.energy.model.EnergyPrice;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class EnergySystemManager {
    private static final String DEFAULT_DB_PATH = "photovoltaic-data.db";
    private static final long UPDATE_INTERVAL_HOURS = 12;

    private final EnergyPricesProvider provider;
    private final EnergyPricesStore store;
    private final ScheduledExecutorService scheduler;
    private final ReentrantLock dbLock = new ReentrantLock();

    public EnergySystemManager() {
        this.provider = new REEEnergyProvider();
        this.store = new SQLiteEnergyPriceStore(Paths.get(System.getProperty("user.dir"), DEFAULT_DB_PATH).toString());
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    public void start() {
        scheduler.scheduleAtFixedRate(
                this::updatePrices,
                0,
                UPDATE_INTERVAL_HOURS,
                TimeUnit.HOURS
        );
    }

    private void updatePrices() {
        LocalDate today = LocalDate.now();
        try {
            List<EnergyPrice> prices = provider.getEnergyPrices(today);
            dbLock.lock();
            try {
                store.saveEnergyPrices(prices);
                System.out.println("Precios actualizados: " + LocalDateTime.now());
            } finally {
                dbLock.unlock();
            }
        } catch (IOException e) {
            System.err.println("Error al actualizar precios: " + e.getMessage());
        }
    }

    public void stop() {
        scheduler.shutdown();
    }
}