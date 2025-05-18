package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Location;
import org.messiyronaldo.weather.model.Weather;
import java.util.List;

public interface WeatherProvider {
	List<Weather> getWeatherForecasts(Location location) throws Exception;
}