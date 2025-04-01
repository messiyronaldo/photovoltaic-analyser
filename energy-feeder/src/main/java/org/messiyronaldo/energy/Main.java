package org.messiyronaldo.energy;

import org.messiyronaldo.energy.control.EnergySystemManager;

public class Main {
    public static void main(String[] args) {
        EnergySystemManager manager = new EnergySystemManager();
        manager.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            manager.stop();
            System.out.println("Sistema de energía detenido.");
        }));

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}