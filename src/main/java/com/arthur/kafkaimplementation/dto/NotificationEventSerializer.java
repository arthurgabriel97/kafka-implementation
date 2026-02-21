package com.arthur.kafkaimplementation.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;

public class NotificationEventSerializer implements Serializer<NotificationEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public byte[] serialize(String topic, NotificationEvent event) {
        if (event == null) return null;
        try {
            return MAPPER.writeValueAsBytes(event);
        } catch (Exception e) {
            throw new RuntimeException("Falha ao serializar NotificationEvent", e);
        }
    }
}
