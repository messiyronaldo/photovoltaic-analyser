package org.messiyronaldo.energy;

import org.messiyronaldo.energy.control.*;

public class Main {
    private static final long UPDATE_INTERVAL_MINUTES = 60 * 12; // 12 hours
    private static final String DATABASE_FILENAME = "energy-data.db";
    private static EnergyController energyController;

    public static void main(String[] args) {
        EnergyPricesProvider energyProvider = new REEEnergyProvider();
        EnergyPricesStore energyStore = new SQLiteEnergyPriceStore(DATABASE_FILENAME);
        EnergyPublisher energyPublisher = new EnergyPublisher();

        System.out.println("Starting energy price monitoring");

        energyController = new EnergyController(
                energyProvider,
                energyStore,
                energyPublisher,
                UPDATE_INTERVAL_MINUTES);

        System.out.println("Energy price controller started");
        System.out.println("Application running. Data will update every " +
                UPDATE_INTERVAL_MINUTES + " minutes.");

        registerShutdownHook();
        keepApplicationRunning();
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down energy-feeder application...");
            shutdownController();
        }));
    }

    private static void shutdownController() {
        if (energyController != null) {
            energyController.shutdown();
            System.out.println("Energy controller shut down successfully");
        }
    }

    private static void keepApplicationRunning() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Application terminated.");
        }
    }
}