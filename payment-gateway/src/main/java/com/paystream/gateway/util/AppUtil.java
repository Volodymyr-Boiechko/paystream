package com.paystream.gateway.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.paystream.gateway.config.AppConfig;

public class AppUtil {

    private AppUtil() {
    }

    public static String getJsonFromObject(Object object) {
        try {
            var objectMapper = AppConfig.getObjectMapper();
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
