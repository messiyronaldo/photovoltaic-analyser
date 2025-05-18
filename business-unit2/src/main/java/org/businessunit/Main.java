package org.businessunit;

import org.businessunit.control.BrokerSubscriber;
import org.businessunit.control.BusinessUnitApplication;

public class Main {
    public static void main(String[] args) {
        System.out.println("Iniciando sistema de generación de datamarts...");

        // Opcional: Iniciar suscriptor de mensajes
        try {
            new BrokerSubscriber().startListening();
        } catch (Exception e) {
            System.err.println("Error en el broker: " + e.getMessage());
        }

        // Iniciar aplicación principal
        BusinessUnitApplication.start();
    }
}