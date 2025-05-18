package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Weather;

public interface WeatherPublisher {
	void start();
	void publish(Weather event);
	void close();
}
