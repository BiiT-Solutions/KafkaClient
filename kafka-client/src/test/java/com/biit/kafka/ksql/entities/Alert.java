package com.biit.kafka.ksql.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Alert {

    @JsonProperty(value = "SENSOR_ID")
    private String sensorId;

    @JsonProperty(value = "START_PERIOD")
    private String startPeriod;

    @JsonProperty(value = "END_PERIOD")
    private String endPeriod;

    @JsonProperty(value = "AVERAGE_READING")
    private double averageReading;

    public Alert() {
    }

    public Alert(String sensorId, String startPeriod, String endPeriod, double averageReading) {
        this.sensorId = sensorId;
        this.startPeriod = startPeriod;
        this.endPeriod = endPeriod;
        this.averageReading = averageReading;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getStartPeriod() {
        return startPeriod;
    }

    public void setStartPeriod(String startPeriod) {
        this.startPeriod = startPeriod;
    }

    public String getEndPeriod() {
        return endPeriod;
    }

    public void setEndPeriod(String endPeriod) {
        this.endPeriod = endPeriod;
    }

    public double getAverageReading() {
        return averageReading;
    }

    public void setAverageReading(double averageReading) {
        this.averageReading = averageReading;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Alert alert)) return false;
        return Double.compare(alert.getAverageReading(), getAverageReading()) == 0 && Objects.equals(getSensorId(), alert.getSensorId()) && Objects.equals(getStartPeriod(), alert.getStartPeriod()) && Objects.equals(getEndPeriod(), alert.getEndPeriod());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSensorId(), getStartPeriod(), getEndPeriod(), getAverageReading());
    }
}
