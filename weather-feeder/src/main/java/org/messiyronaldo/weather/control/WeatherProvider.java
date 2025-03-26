package org.messiyronaldo.weather.control;

import org.messiyronaldo.weather.model.Weather;
import org.messiyronaldo.weather.model.Location;
import java.io.IOException;
import java.util.List;

public interface WeatherProvider {
	List<Weather> getHourlyForecast(Location location) throws IOException;
}