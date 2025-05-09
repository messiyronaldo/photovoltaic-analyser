package org.messiyronaldo.weather.model;

import java.time.Instant;

public class Weather {
	private final Instant ts;
	private final Location location;
	private final Instant predictionTimestamp;
	private final double temperature;
	private final int humidity;
	private final int weatherID;
	private final String weatherMain;
	private final String weatherDescription;
	private final int cloudiness;
	private final double windSpeed;
	private final double rainVolume;
	private final double snowVolume;
	private final String partOfDay;
	private final String ss;

	public Weather(Instant ts, Location location, Instant predictionTimestamp,
				   double temperature, int humidity, int weatherID, String weatherMain,
				   String weatherDescription, int cloudiness, double windSpeed,
				   double rainVolume, double snowVolume, String partOfDay, String ss) {
		this.ts = ts;
		this.location = location;
		this.predictionTimestamp = predictionTimestamp;
		this.temperature = temperature;
		this.humidity = humidity;
		this.weatherID = weatherID;
		this.weatherMain = weatherMain;
		this.weatherDescription = weatherDescription;
		this.cloudiness = cloudiness;
		this.windSpeed = windSpeed;
		this.rainVolume = rainVolume;
		this.snowVolume = snowVolume;
		this.partOfDay = partOfDay;
		this.ss = ss;
	}

	public Instant getTs() {
		return ts;
	}

	public Location getLocation() {
		return location;
	}

	public Instant getPredictionTimestamp() {
		return predictionTimestamp;
	}

	public double getTemperature() {
		return temperature;
	}

	public int getHumidity() {
		return humidity;
	}

	public int getWeatherID() {
		return weatherID;
	}

	public String getWeatherMain() {
		return weatherMain;
	}

	public String getWeatherDescription() {
		return weatherDescription;
	}

	public int getCloudiness() {
		return cloudiness;
	}

	public double getWindSpeed() {
		return windSpeed;
	}

	public double getRainVolume() {
		return rainVolume;
	}

	public double getSnowVolume() {
		return snowVolume;
	}

	public String getPartOfDay() {
		return partOfDay;
	}

	public String getSs() {
		return ss;
	}

	@Override
	public String toString() {
		return "Weather{" +
				"timestamp=" + ts +
				", location=" + location +
				", predictionTimestamp=" + predictionTimestamp +
				", temperature=" + temperature +
				", humidity=" + humidity +
				", weatherID=" + weatherID +
				", weatherMain='" + weatherMain + '\'' +
				", weatherDescription='" + weatherDescription + '\'' +
				", cloudiness=" + cloudiness +
				", windSpeed=" + windSpeed +
				", rainVolume=" + rainVolume +
				", snowVolume=" + snowVolume +
				", partOfDay='" + partOfDay + '\'' +
				", sourceSystem='" + ss + '\'' +
				'}';
	}
}