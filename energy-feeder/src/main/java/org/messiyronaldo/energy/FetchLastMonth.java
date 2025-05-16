package org.messiyronaldo.energy;

import org.messiyronaldo.energy.control.REEEnergyProvider;
import org.messiyronaldo.energy.control.EnergyPublisher;
import org.messiyronaldo.energy.model.EnergyPrice;

import java.time.LocalDate;
import java.util.List;

public class FetchLastMonth {
    public static void main(String[] args) throws Exception {
        REEEnergyProvider provider = new REEEnergyProvider();
        EnergyPublisher publisher = new EnergyPublisher();
        publisher.start();

        LocalDate today = LocalDate.now();
        LocalDate start = today.minusYears(1);
        LocalDate end = today;

        for (LocalDate date = start; !date.isAfter(end); date = date.plusDays(1)) {
            List<EnergyPrice> prices = provider.getEnergyPrices(date);
            for (EnergyPrice price : prices) {
                publisher.publish(price);
            }
            System.out.println("Published for " + date + ": " + prices.size() + " records");
            Thread.sleep(1000);
        }

        publisher.close();
    }
}