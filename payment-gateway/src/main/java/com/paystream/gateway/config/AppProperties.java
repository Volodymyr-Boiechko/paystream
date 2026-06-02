package com.paystream.gateway.config;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class AppProperties {

    private static final Properties APP_PROPERTIES = new Properties();

    static {
        APP_PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        APP_PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        APP_PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }

    private AppProperties() {
    }

    public static Properties get() {
        return APP_PROPERTIES;
    }
}
