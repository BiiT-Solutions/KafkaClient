package com.biit.kafka.events;

public enum EventCustomProperties {

    ISSUER("issuer"),

    FACT_TYPE("factType"),

    ORGANIZATION("organization");

    private final String tag;

    EventCustomProperties(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }
}
