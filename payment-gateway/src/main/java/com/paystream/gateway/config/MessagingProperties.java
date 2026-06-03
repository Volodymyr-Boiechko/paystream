package com.paystream.gateway.config;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessagingProperties {

    private static final Properties MESSAGING_PRODUCER_PROPERTIES = new Properties();

    static {
        MESSAGING_PRODUCER_PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        MESSAGING_PRODUCER_PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        MESSAGING_PRODUCER_PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
    }

    private MessagingProperties() {
    }

    public static Properties get() {
        return MESSAGING_PRODUCER_PROPERTIES;
    }
}
