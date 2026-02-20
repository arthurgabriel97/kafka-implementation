package com.arthur.kafkaimplementation.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

public class NotificationEventDeserializer implements Deserializer<NotificationEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Override
    public NotificationEvent deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return MAPPER.readValue(data, NotificationEvent.class);
        } catch (Exception e) {
            throw new RuntimeException("Falha ao desserializar NotificationEvent", e);
        }
    }
}
