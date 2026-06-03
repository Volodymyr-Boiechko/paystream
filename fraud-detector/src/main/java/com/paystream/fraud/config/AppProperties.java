package com.paystream.fraud.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class AppProperties {

    private static final Properties APP_PROPERTIES = new Properties();

    static {
        APP_PROPERTIES.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        APP_PROPERTIES.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        APP_PROPERTIES.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        APP_PROPERTIES.put(ConsumerConfig.GROUP_ID_CONFIG, "fraud-detector");
        APP_PROPERTIES.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        APP_PROPERTIES.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    private AppProperties() {
    }

    public static Properties get() {
        return APP_PROPERTIES;
    }
}
