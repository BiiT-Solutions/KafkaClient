package com.biit.kafka.controller;

import com.biit.kafka.events.IEventSender;
import com.biit.server.controller.converters.ElementConverter;
import com.biit.server.controllers.models.ElementDTO;
import com.biit.server.converters.models.ConverterRequest;
import com.biit.server.persistence.entities.Element;
import com.biit.server.persistence.repositories.ElementRepository;
import com.biit.server.providers.ElementProvider;

public abstract class KafkaElementController<ENTITY extends Element<KEY>, KEY, DTO extends ElementDTO<KEY>, REPOSITORY extends ElementRepository<ENTITY, KEY>,
        PROVIDER extends ElementProvider<ENTITY, KEY, REPOSITORY>, CONVERTER_REQUEST extends ConverterRequest<ENTITY>,
        CONVERTER extends ElementConverter<ENTITY, DTO, CONVERTER_REQUEST>>
        extends KafkaCreatedElementController<ENTITY, KEY, DTO, REPOSITORY, PROVIDER, CONVERTER_REQUEST, CONVERTER> {

    protected KafkaElementController(PROVIDER provider, CONVERTER converter, IEventSender<DTO> eventSender) {
        super(provider, converter, eventSender);
    }
}
