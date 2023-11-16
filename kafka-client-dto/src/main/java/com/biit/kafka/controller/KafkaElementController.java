package com.biit.kafka.controller;

import com.biit.kafka.events.EventSubject;
import com.biit.kafka.events.IEventSender;
import com.biit.server.controller.ElementController;
import com.biit.server.controller.converters.ElementConverter;
import com.biit.server.controllers.models.ElementDTO;
import com.biit.server.converters.models.ConverterRequest;
import com.biit.server.persistence.entities.Element;
import com.biit.server.persistence.repositories.ElementRepository;
import com.biit.server.providers.ElementProvider;
import jakarta.transaction.Transactional;

import java.util.Collection;
import java.util.List;

public abstract class KafkaElementController<ENTITY extends Element<KEY>, KEY, DTO extends ElementDTO<KEY>, REPOSITORY extends ElementRepository<ENTITY, KEY>,
        PROVIDER extends ElementProvider<ENTITY, KEY, REPOSITORY>, CONVERTER_REQUEST extends ConverterRequest<ENTITY>,
        CONVERTER extends ElementConverter<ENTITY, DTO, CONVERTER_REQUEST>>
        extends ElementController<ENTITY, KEY, DTO, REPOSITORY, PROVIDER, CONVERTER_REQUEST, CONVERTER> {

    private final IEventSender<DTO> eventSender;

    protected KafkaElementController(PROVIDER provider, CONVERTER converter, IEventSender<DTO> eventSender) {
        super(provider, converter);
        this.eventSender = eventSender;
    }

    @Transactional
    public DTO create(DTO dto, String creatorName) {
        final DTO storedDTO = super.create(dto, creatorName);
        eventSender.sendEvents(storedDTO, EventSubject.CREATED, creatorName);
        return storedDTO;
    }

    @Transactional
    public List<DTO> create(Collection<DTO> dtos, String creatorName) {
        final List<DTO> storedDTOs = super.create(dtos, creatorName);
        storedDTOs.forEach(DTO -> eventSender.sendEvents(DTO, EventSubject.CREATED, creatorName));
        return storedDTOs;
    }

    @Transactional
    public void delete(DTO entity, String deletedBy) {

    }
}
