package com.paystream.ledger.messaging.producer;

import com.paystream.ledger.config.MessagingConfig;
import com.paystream.ledger.dto.LedgerEntry;
import com.paystream.ledger.util.AppUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerUpdatedProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LedgerUpdatedProducer.class);

    private static final String TOPIC = "ledger.updated";

    private final KafkaProducer<String, String> producer;

    public LedgerUpdatedProducer() {
        this.producer = new KafkaProducer<>(MessagingConfig.getProducerProperties());
    }

    public void sendLedgerEntry(LedgerEntry ledgerEntry) {

        String json = AppUtil.getJsonFromObject(ledgerEntry);
        String key = ledgerEntry.accountId();

        ProducerRecord<String, String> record = new ProducerRecord<>(
            TOPIC, key, json
        );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("send failed for {}", ledgerEntry.accountId(), exception);
            }
        });
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
    }
}
