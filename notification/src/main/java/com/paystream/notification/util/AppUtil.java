package com.paystream.notification.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.paystream.notification.config.AppConfig;

public class AppUtil {

    private AppUtil() {
    }

    public static <T> T readFromJson(String json, Class<T> clazz) {
        try {
            var objectMapper = AppConfig.getObjectMapper();
            return objectMapper.readValue(json, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

}
