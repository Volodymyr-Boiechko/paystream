package com.paystream.ledger.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.paystream.ledger.config.MessagingConfig;
import com.paystream.ledger.dto.FraudResult;
import com.paystream.ledger.messaging.producer.LedgerDlqProducer;
import com.paystream.ledger.messaging.producer.LedgerRetryProducer;
import com.paystream.ledger.service.LedgerUpdaterService;
import com.paystream.ledger.util.AppUtil;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractLedgerConsumer implements LedgerConsume, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AbstractLedgerConsumer.class);

    private static final int MAX_RETRY_COUNT = 3;

    private volatile boolean running = true;
    private final String subscribeTopic;

    private final KafkaConsumer<String, String> consumer;
    private final LedgerUpdaterService ledgerUpdaterService;
    private final LedgerRetryProducer retryProducer;
    private final LedgerDlqProducer ledgerDlqProducer;

    public AbstractLedgerConsumer(String subscribeTopic, String groupId) {
        this.subscribeTopic = subscribeTopic;
        this.consumer = new KafkaConsumer<>(MessagingConfig.getConsumerProperties(groupId));
        this.ledgerUpdaterService = new LedgerUpdaterService();
        this.retryProducer = new LedgerRetryProducer();
        this.ledgerDlqProducer = new LedgerDlqProducer();
    }

    @Override
    public void consume() {
        consumer.subscribe(List.of(subscribeTopic));

        int count = 0;

        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    var payload = record.value();
                    var fraudResult = AppUtil.readFromJson(payload, FraudResult.class);

                    ledgerUpdaterService.process(fraudResult);

                    if (count++ % 1000 == 0) {
                        log.info("processed {}, partition {} offset {}", count, record.partition(), record.offset());
                    }
                } catch (JsonProcessingException | IllegalArgumentException e) {
                    ledgerDlqProducer.putInDeadLetterQueue(record, e, "Invalid message format");
                } catch (Exception e) {
                    tryRetry(record, e);
                }
            }

            commitProcessed();
        }
    }

    private void tryRetry(ConsumerRecord<String, String> record, Exception e) {
        int retryCount = getRetryCount(record) + 1;
        if (retryCount <= MAX_RETRY_COUNT) {
            retryProducer.retry(record, retryCount, e);
        } else {
            ledgerDlqProducer.putInDeadLetterQueue(record, e, "Exceeded amount of retries");
        }
    }

    private Integer getRetryCount(ConsumerRecord<String, String> record) {
        for (Header header : record.headers().headers("retry-count")) {
            String value = new String(header.value(), StandardCharsets.UTF_8);
            return Integer.parseInt(value);
        }
        return 0;
    }

    private void commitProcessed() {
        try {
            consumer.commitSync();
        } catch (CommitFailedException e) {
            log.warn("commit failed, partition reassigned: {}", e.getMessage());
        } catch (KafkaException e) {
            log.error("commit error, will retry on next batch", e);
        }
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public void close() {
        consumer.close();
    }

}
