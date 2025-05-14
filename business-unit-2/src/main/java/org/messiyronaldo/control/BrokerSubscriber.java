package org.messiyronaldo.control;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class BrokerSubscriber {
    private static final String BROKER_URL = "tcp://localhost:61616";
    private static final String ENERGY_TOPIC = "energy-prices";
    private static final String WEATHER_TOPIC = "weather-data";
    private final DataMartManager dataMartManager = new DataMartManager();

    public void startListening() throws JMSException {
        ConnectionFactory factory = new ActiveMQConnectionFactory(BROKER_URL);
        Connection connection = factory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Suscribirse a ambos topics
        MessageConsumer energyConsumer = session.createConsumer(session.createTopic(ENERGY_TOPIC));
        MessageConsumer weatherConsumer = session.createConsumer(session.createTopic(WEATHER_TOPIC));

        energyConsumer.setMessageListener(message -> processMessage(message, "ENERGY"));
        weatherConsumer.setMessageListener(message -> processMessage(message, "WEATHER"));
    }

    private void processMessage(Message message, String type) {
        if (message instanceof TextMessage) {
            try {
                String event = ((TextMessage) message).getText();
                System.out.printf("[%s] Evento recibido: %s%n", type, event);
                dataMartManager.saveToDataMart(event);
            } catch (JMSException e) {
                System.err.println("Error procesando mensaje: " + e.getMessage());
            }
        }
    }
}