package com.paystream.ledger.messaging.producer;

import com.paystream.ledger.config.MessagingConfig;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerDlqProducer implements AutoCloseable {

    private static final String TOPIC = "payment.authorized.dlq";
    private static final Logger log = LoggerFactory.getLogger(LedgerDlqProducer.class);

    private final KafkaProducer<String, String> producer;

    public LedgerDlqProducer() {
        this.producer = new KafkaProducer<>(MessagingConfig.getProducerProperties());
    }

    public void putInDeadLetterQueue(ConsumerRecord<String, String> consumerRecord, Exception e, String reason) {

        ProducerRecord<String, String> deadLetterRecord = new ProducerRecord<>(
            TOPIC, consumerRecord.key(), consumerRecord.value()
        );

        deadLetterRecord.headers().add("reason", reason.getBytes(StandardCharsets.UTF_8));
        deadLetterRecord.headers().add("error", e.getMessage().getBytes(StandardCharsets.UTF_8));

        log.warn("Dead Letter Record: {}", deadLetterRecord, e);

        producer.send(deadLetterRecord);
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
