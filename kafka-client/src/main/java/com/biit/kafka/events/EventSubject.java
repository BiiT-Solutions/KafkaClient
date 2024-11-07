package com.biit.kafka.events;

public enum EventSubject {

    CREATE,

    UPDATE,

    DELETE,

    ENABLE,

    DISABLE,

    CREATED,

    UPDATED,

    DELETED,

    ENABLED,

    DISABLED,

    ERROR,

    SUCCESS,

    REPORT;

    public static EventSubject from(String tag) {
        for (EventSubject eventSubject : EventSubject.values()) {
            if (eventSubject.name().equalsIgnoreCase(tag)) {
                return eventSubject;
            }
        }
        return null;
    }

}
