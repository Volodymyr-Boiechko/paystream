package com.paystream.ledger.messaging.producer;

import com.paystream.ledger.config.MessagingConfig;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerRetryProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LedgerRetryProducer.class);

    private static final String TOPIC = "payment.authorized.retry";

    private final KafkaProducer<String, String> producer;

    public LedgerRetryProducer() {
        this.producer = new KafkaProducer<>(MessagingConfig.getProducerProperties());
    }

    public void retry(ConsumerRecord<String, String> consumerRecord, Integer retryCount, Exception e) {

        ProducerRecord<String, String> retryRecord = new ProducerRecord<>(
            TOPIC, consumerRecord.key(), consumerRecord.value()
        );

        retryRecord.headers().add("error", e.getMessage().getBytes(StandardCharsets.UTF_8));
        retryRecord.headers().add("retry-count", retryCount.toString().getBytes(StandardCharsets.UTF_8));

        log.warn("Processing retry record {}, count {}", retryRecord, retryCount, e);

        producer.send(retryRecord);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
