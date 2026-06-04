package com.paystream.ledger.config;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AppConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .disable(WRITE_DATES_AS_TIMESTAMPS)
        .registerModule(new JavaTimeModule());

    private AppConfig() {
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}
