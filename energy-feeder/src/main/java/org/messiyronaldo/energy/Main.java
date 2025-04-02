package org.messiyronaldo.energy;

import org.messiyronaldo.energy.control.*;

public class Main {
    public static void main(String[] args) {
        EnergyPricesProvider energyProvider = new REEEnergyProvider();
        EnergyPricesStore energyStore = new SQLiteEnergyPriceStore("photovoltaic-data.db");

        final long updateIntervalMinutes = 60 * 12;

        System.out.println("Iniciando monitoreo de precios de energía");

        EnergyController energyController = new EnergyController(
                energyProvider, energyStore, updateIntervalMinutes);

        System.out.println("Controlador de precios de energía iniciado");
        System.out.println("Aplicación en ejecución. Los datos se actualizarán cada " +
                updateIntervalMinutes + " minutos.");

        // Mantener la aplicación en ejecución
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            System.out.println("Aplicación finalizada.");
        }
    }
}