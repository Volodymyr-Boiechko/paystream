package com.paystream.fraud.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.paystream.fraud.config.AppConfig;

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

    public static String getJsonFromObject(Object object) {
        try {
            var objectMapper = AppConfig.getObjectMapper();
            return objectMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
