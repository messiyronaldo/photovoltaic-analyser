package org.messiyronaldo.energy;

import org.messiyronaldo.energy.control.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static final long UPDATE_INTERVAL_MINUTES = 60 * 12; // 12 hours
    private static EnergyController energyController;

    public static void main(String[] args) {
        validateArguments(args);

        String databaseFileName = args[0];
        String storeType = args[1].toLowerCase();

        EnergyPricesProvider energyProvider = new REEEnergyProvider();
        EnergyPricesStore energyStore = null;
        EnergyPublisher energyPublisher = null;

        if (storeType.equals("sql")) {
            energyStore = new SQLiteEnergyPriceStore(databaseFileName);
        } else if (storeType.equals("activemq")) {
            energyPublisher = new EnergyPublisher();
            energyPublisher.start();
            logger.info("Energy publisher started successfully");
        } else {
            logger.error("Invalid store type. Use 'sql' or 'activemq'.");
            System.exit(1);
        }

        logger.info("Starting energy price monitoring");

        energyController = new EnergyController(
                energyProvider, energyStore, energyPublisher, UPDATE_INTERVAL_MINUTES);

        logger.info("Energy price controller started");
        logger.info("Application running. Data will update every {} minutes", UPDATE_INTERVAL_MINUTES);

        registerShutdownHook();
        keepApplicationRunning();
    }

    private static void validateArguments(String[] args) {
        if (args.length != 2) {
            logger.error("Invalid arguments. Usage: java -jar energy-feeder.jar <database-file> <store-type>");
            System.exit(1);
        }
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down energy-feeder application...");
            shutdownController();
        }));
    }

    private static void shutdownController() {
        if (energyController != null) {
            energyController.shutdown();
            logger.info("Energy controller shut down successfully");
        }
    }

    private static void keepApplicationRunning() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            logger.info("Application terminated");
        }
    }
}