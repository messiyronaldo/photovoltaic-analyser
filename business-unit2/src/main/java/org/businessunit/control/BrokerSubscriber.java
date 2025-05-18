package org.businessunit.control;

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

        MessageConsumer energyConsumer = session.createConsumer(session.createTopic(ENERGY_TOPIC));
        MessageConsumer weatherConsumer = session.createConsumer(session.createTopic(WEATHER_TOPIC));

        energyConsumer.setMessageListener(message -> handleMessage(message, "ENERGY"));
        weatherConsumer.setMessageListener(message -> handleMessage(message, "WEATHER"));
    }

    private void handleMessage(Message message, String type) {
        try {
            if (message instanceof TextMessage) {
                String content = ((TextMessage) message).getText();
                System.out.printf("[%s] Nuevos datos: %s%n", type, content);
                dataMartManager.saveToDataMart(content);

                if (content.contains("complete_dataset")) {
                    dataMartManager.generateConsolidatedFiles();
                }
            }
        } catch (JMSException e) {
            System.err.println("Error procesando mensaje: " + e.getMessage());
        }
    }
}