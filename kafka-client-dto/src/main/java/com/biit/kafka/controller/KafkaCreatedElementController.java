package com.biit.kafka.controller;

import com.biit.kafka.events.EventSubject;
import com.biit.kafka.events.IEventSender;
import com.biit.server.controller.CreatedElementController;
import com.biit.server.controller.converters.ElementConverter;
import com.biit.server.controllers.models.CreatedElementDTO;
import com.biit.server.converters.models.ConverterRequest;
import com.biit.server.persistence.entities.CreatedElement;
import com.biit.server.persistence.repositories.CreatedElementRepository;
import com.biit.server.providers.CreatedElementProvider;
import jakarta.transaction.Transactional;

import java.util.Collection;
import java.util.List;

public abstract class KafkaCreatedElementController<
        ENTITY extends CreatedElement, KEY, DTO extends CreatedElementDTO,
        REPOSITORY extends CreatedElementRepository<ENTITY, KEY>,
        PROVIDER extends CreatedElementProvider<ENTITY, KEY, REPOSITORY>,
        CONVERTER_REQUEST extends ConverterRequest<ENTITY>,
        CONVERTER extends ElementConverter<ENTITY, DTO, CONVERTER_REQUEST>>
        extends CreatedElementController<ENTITY, KEY, DTO, REPOSITORY, PROVIDER, CONVERTER_REQUEST, CONVERTER> {

    private final IEventSender<DTO> eventSender;

    protected KafkaCreatedElementController(PROVIDER provider, CONVERTER converter, IEventSender<DTO> eventSender) {
        super(provider, converter);
        this.eventSender = eventSender;
    }

    public IEventSender<DTO> getEventSender() {
        return eventSender;
    }

    @Override
    @Transactional
    public DTO update(DTO dto, String updaterName) {
        final DTO storedDTO = super.update(dto, updaterName);
        eventSender.sendEvents(storedDTO, EventSubject.UPDATED, updaterName);
        return storedDTO;
    }

    @Override
    @Transactional
    public DTO create(DTO dto, String creatorName) {
        final DTO storedDTO = super.create(dto, creatorName);
        eventSender.sendEvents(storedDTO, EventSubject.CREATED, creatorName);
        return storedDTO;
    }

    @Override
    @Transactional
    public List<DTO> create(Collection<DTO> dtos, String creatorName) {
        final List<DTO> storedDTOs = super.create(dtos, creatorName);
        storedDTOs.forEach(DTO -> eventSender.sendEvents(DTO, EventSubject.CREATED, creatorName));
        return storedDTOs;
    }

    @Override
    @Transactional
    public void delete(DTO entity, String deletedBy) {
        super.delete(entity, deletedBy);
        eventSender.sendEvents(entity, EventSubject.DELETED, deletedBy);
    }

    @Override
    public void deleteById(KEY id, String deletedBy) {
        final ENTITY entity = getProvider().get(id).orElse(null);
        super.deleteById(id, deletedBy);
        eventSender.sendEvents(convert(entity), EventSubject.DELETED, deletedBy);
    }
}
