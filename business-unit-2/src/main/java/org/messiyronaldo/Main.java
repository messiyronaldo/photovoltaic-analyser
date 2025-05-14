package org.messiyronaldo;

import org.messiyronaldo.control.BrokerSubscriber;
import org.messiyronaldo.control.BusinessUnitApplication;

public class Main {
    public static void main(String[] args) {
        System.out.println("Iniciando Business Unit...");

        try {
            BrokerSubscriber brokerSubscriber = new BrokerSubscriber();
            brokerSubscriber.startListening();
        } catch (Exception e) {
            e.printStackTrace();
        }

        BusinessUnitApplication.start();
    }
}
