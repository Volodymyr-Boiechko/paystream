package com.paystream.notification.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MessagingConfig {

    private static final String BOOTSTRAP_SERVERS_CONFIG =
        System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");

    private static final Properties CONSUMER_PROPERTIES = new Properties();

    static {
        CONSUMER_PROPERTIES.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        CONSUMER_PROPERTIES.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        CONSUMER_PROPERTIES.put(ConsumerConfig.GROUP_ID_CONFIG, "notification");
        CONSUMER_PROPERTIES.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        CONSUMER_PROPERTIES.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    private MessagingConfig() {
    }

    public static Properties getConsumerProperties() {
        return CONSUMER_PROPERTIES;
    }
}
