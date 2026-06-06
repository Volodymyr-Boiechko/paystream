package com.paystream.fraud.messaging.producer;

import com.paystream.fraud.config.MessagingConfig;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDlqProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FraudDlqProducer.class);

    private static final String TOPIC = "payment.requested.dlq";
    private static final String FIRST_RETRY_COUNT = "1";

    private final KafkaProducer<String, String> producer;

    public FraudDlqProducer() {
        this.producer = new KafkaProducer<>(MessagingConfig.getProducerProperties());
    }

    public void putInDeadLetterQueue(ConsumerRecord<String, String> record, Exception e) {

        ProducerRecord<String, String> dlqRecord = new ProducerRecord<>(TOPIC, record.key(), record.value());

        dlqRecord.headers().add("error", e.getMessage().getBytes(StandardCharsets.UTF_8));
        dlqRecord.headers().add("retry-count", FIRST_RETRY_COUNT.getBytes(StandardCharsets.UTF_8));

        log.warn("Processing dlq record {}", dlqRecord, e);

        producer.send(dlqRecord);
    }


    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
