package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Weather;
import java.util.List;

public interface WeatherStore {
	void saveWeatherForecasts(List<Weather> weatherList);
	List<Weather> getWeatherForecasts(double latitude, double longitude);
}