package com.biit.kafka.ksql.entities;

public class Reading {
    private String id;
    private String timestamp;
    private int reading;

    public Reading() {
    }

    public Reading(String id, String timestamp, int reading) {
        this.id = id;
        this.timestamp = timestamp;
        this.reading = reading;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public int getReading() {
        return reading;
    }

    public void setReading(int reading) {
        this.reading = reading;
    }
}
