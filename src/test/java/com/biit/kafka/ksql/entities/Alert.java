package com.biit.kafka.ksql.entities;

/*-
 * #%L
 * Kafka client
 * %%
 * Copyright (C) 2021 - 2025 BiiT Sourcing Solutions S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

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
