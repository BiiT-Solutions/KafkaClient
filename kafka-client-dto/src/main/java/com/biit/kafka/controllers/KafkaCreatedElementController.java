package com.biit.kafka.controllers;

import com.biit.kafka.controllers.models.EventDTO;
import com.biit.kafka.events.EventSubject;
import com.biit.kafka.events.IEventSender;
import com.biit.kafka.logger.KafkaLogger;
import com.biit.server.controller.CreatedElementController;
import com.biit.server.controller.converters.ElementConverter;
import com.biit.server.controllers.models.CreatedElementDTO;
import com.biit.server.converters.models.ConverterRequest;
import com.biit.server.exceptions.ValidateBadRequestException;
import com.biit.server.persistence.entities.CreatedElement;
import com.biit.server.persistence.repositories.CreatedElementRepository;
import com.biit.server.providers.CreatedElementProvider;
import com.biit.server.security.IUserOrganizationProvider;
import com.biit.server.security.model.IUserOrganization;
import jakarta.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public abstract class KafkaCreatedElementController<
        ENTITY extends CreatedElement, KEY, DTO extends CreatedElementDTO,
        REPOSITORY extends CreatedElementRepository<ENTITY, KEY>,
        PROVIDER extends CreatedElementProvider<ENTITY, KEY, REPOSITORY>,
        CONVERTER_REQUEST extends ConverterRequest<ENTITY>,
        CONVERTER extends ElementConverter<ENTITY, DTO, CONVERTER_REQUEST>>
        extends CreatedElementController<ENTITY, KEY, DTO, REPOSITORY, PROVIDER, CONVERTER_REQUEST, CONVERTER> {

    private final IEventSender<DTO> eventSender;

    protected KafkaCreatedElementController(PROVIDER provider, CONVERTER converter, @Autowired(required = false) IEventSender<DTO> eventSender,
                                            List<IUserOrganizationProvider<? extends IUserOrganization>> userOrganizationProvider) {
        super(provider, converter, userOrganizationProvider);
        this.eventSender = eventSender;
    }

    public IEventSender<DTO> getEventSender() {
        return eventSender;
    }

    @Override
    @Transactional
    public DTO update(DTO dto, String updaterName) {
        final DTO storedDTO = super.update(dto, updaterName);
        if (eventSender != null) {
            eventSender.sendEvents(storedDTO, EventSubject.UPDATED, updaterName);
        }
        return storedDTO;
    }

    @Override
    @Transactional
    public DTO create(DTO dto, String creatorName) {
        validate(dto);
        final DTO storedDTO = super.create(dto, creatorName);
        if (eventSender != null) {
            eventSender.sendEvents(storedDTO, EventSubject.CREATED, creatorName);
        }
        return storedDTO;
    }

    @Override
    @Transactional
    public List<DTO> create(Collection<DTO> dtos, String creatorName) {
        validate(dtos);
        final List<DTO> storedDTOs = super.create(dtos, creatorName);
        if (eventSender != null) {
            storedDTOs.forEach(DTO -> eventSender.sendEvents(DTO, EventSubject.CREATED, creatorName));
        }
        return storedDTOs;
    }

    @Override
    @Transactional
    public void delete(DTO entity, String deletedBy) {
        super.delete(entity, deletedBy);
        if (eventSender != null) {
            eventSender.sendEvents(entity, EventSubject.DELETED, deletedBy);
        }
    }

    @Override
    public void deleteById(KEY id, String deletedBy) {
        final ENTITY entity = getProvider().get(id).orElse(null);
        super.deleteById(id, deletedBy);
        if (eventSender != null) {
            eventSender.sendEvents(convert(entity), EventSubject.DELETED, deletedBy);
        }
    }

    @Override
    public void deleteAll(String deletedBy) {
        final Collection<ENTITY> entities = getProvider().getAll();
        super.deleteAll(deletedBy);
        if (eventSender != null) {
            entities.forEach(entity -> {
                try {
                    eventSender.sendEvents(convert(entity), EventSubject.DELETED, deletedBy);
                } catch (Exception e) {
                    KafkaLogger.severe(this.getClass(), "Event failure: 'DELETE' on entity " + entity);
                    KafkaLogger.errorMessage(this.getClass(), e);
                }
            });
        }
    }


    public void validate(EventDTO eventDTO) throws ValidateBadRequestException {
        if (eventDTO.getMessageId() == null) {
            eventDTO.setMessageId(UUID.randomUUID());
        }
        if (eventDTO.getCreatedAt() == null) {
            eventDTO.setCreatedAt(LocalDateTime.now());
        }
    }
}
