package com.biit.kafka.events;

public enum EventCustomProperties {

    ISSUER("issuer"),

    FACT_TYPE("factType"),

    //Label from the source.
    SOURCE_TAG("sourceTag"),

    ORGANIZATION("organization");

    private final String tag;

    EventCustomProperties(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }
}
