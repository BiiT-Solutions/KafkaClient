package com.biit.kafka.exceptions;

import com.biit.kafka.logger.KafkaLogger;
import org.springframework.kafka.support.serializer.FailedDeserializationInfo;

import java.util.function.Function;

public class FailedEventDeserializer implements Function<FailedDeserializationInfo, Object> {

    @Override
    public Object apply(FailedDeserializationInfo info) {
        KafkaLogger.severe(this.getClass().getName(), info.toString());
        return null;
    }

}
