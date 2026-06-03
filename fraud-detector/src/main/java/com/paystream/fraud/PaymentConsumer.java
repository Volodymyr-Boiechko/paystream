package com.paystream.fraud;

import com.paystream.fraud.config.AppProperties;
import com.paystream.fraud.dto.PaymentRequested;
import com.paystream.fraud.util.AppUtil;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaymentConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumer.class);

    private static final String TOPIC = "payment.requested";

    private final KafkaConsumer<String, String> consumer;

    public PaymentConsumer() {
        this.consumer = new KafkaConsumer<>(AppProperties.get());
    }

    public void consume() {
        consumer.subscribe(List.of(TOPIC));

        int count = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                var payload = record.value();
                var paymentRequested = AppUtil.readFromJson(payload, PaymentRequested.class);

                count++;
                if (count % 1000 == 0) {
                    log.info("Payment requested: {}", paymentRequested);
                    log.info("processed {}, partition {} offset {}", count, record.partition(), record.offset());
                }
            }

            commitProcessed();
        }
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
    public void close() {
        consumer.close();
    }
}
