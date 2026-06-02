package com.paystream.gateway.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class AppConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    private AppConfig() {
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}
