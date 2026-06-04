package com.paystream.ledger.config;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessagingConfig {

    private static final Properties MESSAGING_CONSUMER_PROPERTIES = new Properties();
    private static final Properties MESSAGING_PRODUCER_PROPERTIES = new Properties();

    static {
        MESSAGING_CONSUMER_PROPERTIES.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        MESSAGING_CONSUMER_PROPERTIES.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        MESSAGING_CONSUMER_PROPERTIES.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        MESSAGING_CONSUMER_PROPERTIES.put(ConsumerConfig.GROUP_ID_CONFIG, "ledger");
        MESSAGING_CONSUMER_PROPERTIES.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        MESSAGING_CONSUMER_PROPERTIES.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }

    static {
        MESSAGING_PRODUCER_PROPERTIES.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        MESSAGING_PRODUCER_PROPERTIES.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        MESSAGING_PRODUCER_PROPERTIES.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class.getName());
    }

    private MessagingConfig() {
    }

    public static Properties getConsumerProperties() {
        return MESSAGING_CONSUMER_PROPERTIES;
    }

    public static Properties getProducerProperties() {
        return MESSAGING_PRODUCER_PROPERTIES;
    }
}
